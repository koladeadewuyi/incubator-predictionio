/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.predictionio.data.storage.cassandra

import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.storage.EventValidation
import org.apache.predictionio.data.storage.DataMap

import org.json4s.DefaultFormats
import org.json4s.JObject
import org.json4s.native.Serialization.{ read, write }

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import org.apache.commons.codec.binary.Base64
import java.security.MessageDigest
import java.nio.ByteBuffer

import com.datastax.driver.core._


import java.util.UUID

case class KeyedEvent(
                       eventId: String,
                       event: String,
                       entityType: String,
                       entityId: String,
                       targetEntityType: Option[String],
                       targetEntityId: Option[String],
                       properties: String,
                       prId: Option[String],
                       eventTime: DateTime,
                       eventTimeZone: String,
                       creationTime: DateTime,
                       creationTimeZone: String
                       )

/* common utility function for accessing EventsStore in Cassandra */
object CSEventsUtil {

  implicit val formats = DefaultFormats

  var statements: Map[String, PreparedStatement] = Map.empty[String, PreparedStatement]

  private def optionalColumn(param: Boolean, column: String, default: String = ""): String = {
    if (param) s", $column" else default
  }

  private def optionalPlaceHolder(param: Boolean, placeholder: String, default: String = ""): String = {
    if (param) s", :$placeholder" else default
  }

  private def prepareStatement(client: Session,
                               tableName: String,
                               prId: Boolean,
                               targetEntityType: Boolean,
                               targetEntityId: Boolean): PreparedStatement = {
    val query =
      s"""
      |insert into $tableName(
      |eventId,
      |event,
      |entityType,
      |entityId,
      |properties
      |${optionalColumn(prId, "prId")},
      |eventTime,
      |eventTimeZone,
      |creationTime,
      |creationTimeZone
      | ${optionalColumn(targetEntityType, "targetEntityType")}
      | ${optionalColumn(targetEntityId, "targetEntityId")}
      |) values (
      |:eventId,
      |:event,
      |:entityType,
      |:entityId,
      |:properties
      | ${optionalPlaceHolder(prId, "prId")},
      |:eventTime,
      |:eventTimeZone,
      |:creationTime,
      |:creationTimeZone
      | ${optionalPlaceHolder(targetEntityType, "targetEntityType")}
      | ${optionalPlaceHolder(targetEntityId, "targetEntityId")}
      |)
      """.stripMargin
    client.prepare(query)
  }

  def getTableName(namespace: String, appId: Int, channelId: Option[Int] = None): String = {
    channelId.map { ch =>
      s"${namespace}_${appId}_$ch"
    }.getOrElse {
      s"${namespace}_$appId"
    }
  }

  def getInsertStatement(client: Session, keySpace: String, appId: Int, channelId: Option[Int], keyedEvent: KeyedEvent): Option[PreparedStatement] = {
    val tableName = getTableName(keySpace, appId, channelId)
    val key = s"${keyedEvent.prId.isDefined}_${keyedEvent.targetEntityType.isDefined}_${keyedEvent.targetEntityId.isDefined}"
    if (statements.isEmpty) {
      val biValue = List(true, false)
      statements = biValue.map { prId =>
        biValue.map { targetEntityType =>
          biValue.map { targetEntityId =>
            Map(s"${prId}_${targetEntityType}_$targetEntityId" -> prepareStatement(client, tableName, prId, targetEntityType, targetEntityId))
          }.flatten
        }.flatten
      }.flatten.toMap
    }

    statements.get(key)
  }

  def hash(entityType: String, entityId: String): Array[Byte] = {
    val s = entityType + "-" + entityId
    // get a new MessageDigest object each time for thread-safe
    val md5 = MessageDigest.getInstance("MD5")
    md5.digest(s.toCharArray.map(_.toByte))
  }

  class RowKey(
    val b: Array[Byte]
  ) {
    require((b.size == 32), s"Incorrect b size: ${b.size}")
    lazy val entityHash: Array[Byte] = b.slice(0, 16)
    lazy val millis: Long = ByteBuffer.wrap(b.slice(16, 24)).getLong
    lazy val uuidLow: Long = ByteBuffer.wrap(b.slice(24, 32)).getLong

    lazy val toBytes: Array[Byte] = b

    override def toString: String = {
      Base64.encodeBase64URLSafeString(toBytes)
    }
  }

  object RowKey {
    def apply(
      entityType: String,
      entityId: String,
      millis: Long,
      uuidLow: Long): RowKey = {
        // add UUID least significant bits for multiple actions at the same time
        // (UUID's most significant bits are actually timestamp,
        // use eventTime instead).
        val b = hash(entityType, entityId) ++
          ByteBuffer.allocate(8).putLong(millis).array ++ ByteBuffer.allocate(8).putLong(uuidLow).array
        new RowKey(b)
      }

    // get RowKey from string representation
    def apply(s: String): RowKey = {
      try {
        apply(Base64.decodeBase64(s))
      } catch {
        case e: Exception => throw new RowKeyException(
          s"Failed to convert String ${s} to RowKey because ${e}", e)
      }
    }

    def apply(b: Array[Byte]): RowKey = {
      if (b.size != 32) {
        val bString = b.mkString(",")
        throw new RowKeyException(
          s"Incorrect byte array size. Bytes: ${bString}.")
      }
      new RowKey(b)
    }

  }

  class RowKeyException(val msg: String, val cause: Exception)
    extends Exception(msg, cause) {
      def this(msg: String) = this(msg, null)
    }

  def eventToPut(event: Event, appId: Int): KeyedEvent = {
    // generate new rowKey if eventId is None
    val rowKey: RowKey = event.eventId.map { id =>
      RowKey(id) // create rowKey from eventId
    }.getOrElse {
      // TOOD: use real UUID. not pseudo random
      val uuidLow: Long = UUID.randomUUID().getLeastSignificantBits
      RowKey(
        entityType = event.entityType,
        entityId = event.entityId,
        millis = event.eventTime.getMillis,
        uuidLow = uuidLow
      )
    }

    def validateTimeZone(eventTimeZone: DateTimeZone): String = {
      if (!eventTimeZone.equals(EventValidation.defaultTimeZone)) {
        eventTimeZone.getID
      } else eventTimeZone.getID
    }

    def getProperties(eventProperties: DataMap): String = {
      if (!event.properties.isEmpty) {
        write(event.properties.toJObject())
      } else write(new DataMap(Map.empty).toJObject())
    }

    val keyedEvent = KeyedEvent(
      rowKey.toString,
      event.event,
      event.entityType,
      event.entityId,
      event.targetEntityType,
      event.targetEntityId,
      getProperties(event.properties),
      event.prId,
      event.eventTime,
      validateTimeZone(event.eventTime.getZone),
      event.creationTime,
      validateTimeZone(event.creationTime.getZone)
    )

    keyedEvent
  }

  def resultToEvent(result: Row, appId: Int): Event = {
    val rowKey = Some(result.get("eventId", classOf[String]))
    val event = result.get("event", classOf[String])
    val entityType = result.get("entityType", classOf[String])
    val entityId = result.get("entityId", classOf[String])
    val targetEntityType = Option(result.get("targetEntityType", classOf[String]))
    val targetEntityId = Option(result.get("targetEntityId", classOf[String]))
    val properties: DataMap = Option(result.get("properties", classOf[String]))
      .map(s => DataMap(read[JObject](s))).getOrElse(DataMap())
    val prId = Option(result.get("prId", classOf[String]))
    val eventTimeZone = Option(result.get("eventTimeZone", classOf[String]))
      .map(DateTimeZone.forID(_))
      .getOrElse(EventValidation.defaultTimeZone)
    val eventTime = new DateTime(
      result.getTimestamp("eventTime").getTime, eventTimeZone)
    val creationTimeZone = Option(result.get("creationTimeZone", classOf[String]))
      .map(DateTimeZone.forID(_))
      .getOrElse(EventValidation.defaultTimeZone)
    val creationTime: DateTime = new DateTime(
      result.getTimestamp("creationTime").getTime, creationTimeZone)

    Event(
      eventId = rowKey,
      event = event,
      entityType = entityType,
      entityId = entityId,
      targetEntityType = targetEntityType,
      targetEntityId = targetEntityId,
      properties = properties,
      eventTime = eventTime,
      tags = Seq(),
      prId = prId,
      creationTime = creationTime
    )
  }
}
