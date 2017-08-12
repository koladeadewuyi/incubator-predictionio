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

import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.github.nscala_time.time.Imports._
import org.apache.predictionio.data.storage._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTimeZone, DateTime}
import org.json4s.{DefaultFormats, JObject}
import org.json4s.native.Serialization.read

class CSPEvents(client: Session, config: StorageClientConfig, keySpace: String) extends PEvents {

  def checkTableExists(appId: Int, channelId: Option[Int]): Unit = {

    val tableName = CSEventsUtil.getTableName(keySpace, appId, channelId)

    val result = client.execute(
      s"select table_name from system_schema.tables where keyspace_name='$keySpace'"
    ).getColumnDefinitions
    if (result.contains(tableName)) {
      if (channelId.nonEmpty) {
        logger.error(s"The appId $appId with channelId $channelId does not exist." +
          s" Please use valid appId and channelId.")
        throw new Exception(s"Cassandra table not found for appId $appId" +
          s" with channelId $channelId.")
      } else {
        logger.error(s"The appId $appId does not exist. Please use valid appId.")
        throw new Exception(s"Cassandra table not found for appId $appId.")
      }
    }
  }

  override
  def find(
    appId: Int,
    channelId: Option[Int] = None,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    entityType: Option[String] = None,
    entityId: Option[String] = None,
    eventNames: Option[Seq[String]] = None,
    targetEntityType: Option[Option[String]] = None,
    targetEntityId: Option[Option[String]] = None
    )(sc: SparkContext): RDD[Event] = {

    val tableName = CSEventsUtil.getTableName(keySpace, appId, channelId)
    checkTableExists(appId, channelId)

    val lower = startTime.map(_.getMillis).getOrElse(0.toLong)
    val upper = untilTime.map(_.getMillis).getOrElse((DateTime.now + 1.years).getMillis)
    val eventNamesClause = eventNames.map("and event in (" + _.map(event => s"'$event'").mkString(",") + ")").getOrElse("")

    val entityTypeClause = entityType.map(x => s"and entityType = '$x'").getOrElse("")
    val entityIdClause = entityId.map(x => s"and entityId = '$x'").getOrElse("")

    val targetEntityTypeClause = targetEntityType.map(
      _.map(x => s"and targetEntityType = '$x'"
    ).getOrElse("and targetEntityType is null")).getOrElse("")

    val targetEntityIdClause = targetEntityId.map(
      _.map(x => s"and targetEntityId = '$x'"
    ).getOrElse("and targetEntityId is null")).getOrElse("")

    val cql = s"""
        eventTime >= $lower and
        eventTime < $upper
      $entityTypeClause
      $entityIdClause
      $eventNamesClause
      $targetEntityTypeClause
      $targetEntityIdClause
      """.replace("\n", " ")

    val rdd: RDD[Event] = sc.cassandraTable(keySpace, tableName).where(cql).map {
      row =>
        implicit val formats = DefaultFormats
        val eventTimeZone: DateTimeZone = row.getStringOption("eventtimezone").map(DateTimeZone.forID).getOrElse(EventValidation.defaultTimeZone)
        val creationTime: DateTimeZone = row.getStringOption("creationtimezone").map(DateTimeZone.forID).getOrElse(EventValidation.defaultTimeZone)
        Event(
          row.getStringOption("eventid"),
          row.getString("event"),
          row.getString("entitytype"),
          row.getString("entityid"),
          row.getStringOption("targetentitytype"),
          row.getStringOption("targetentityid"),
          row.getStringOption("properties").map(s => DataMap(read[JObject](s))).getOrElse(DataMap()),
          new DateTime(row.getDate("eventtime"), eventTimeZone),
          tags = Seq(),
          prId = row.getStringOption("prid"),
          new DateTime(row.getDate("creationtime"), creationTime)
        )
    }
    rdd
  }

  override
  def write(
    events: RDD[Event], appId: Int, channelId: Option[Int])(sc: SparkContext): Unit = {

    checkTableExists(appId, channelId)
    val tableName = CSEventsUtil.getTableName(keySpace, appId, channelId)

    val keyedEvents: RDD[KeyedEvent] = events.map { event =>
      val keyedEvent = CSEventsUtil.eventToPut(event, appId)
      KeyedEvent(
        keyedEvent.eventId,
        keyedEvent.event,
        keyedEvent.entityType,
        keyedEvent.entityId,
        keyedEvent.targetEntityType,
        keyedEvent.targetEntityId,
        keyedEvent.properties,
        keyedEvent.prId,
        keyedEvent.eventTime,
        keyedEvent.eventTimeZone,
        keyedEvent.creationTime,
        keyedEvent.creationTimeZone
      )
    }
    keyedEvents.saveToCassandra(keySpace, tableName)
  }

  def delete(
    eventIds: RDD[String], appId: Int, channelId: Option[Int])(sc: SparkContext): Unit = {

    checkTableExists(appId, channelId)

    val tableName = CSEventsUtil.getTableName(keySpace, appId, channelId)
    val rdd = eventIds.map { eventId =>
      CSEventsUtil.RowKey(eventId)
    }

    rdd.foreachPartition(partition => {
      val query = s"delete from $keySpace.$tableName where eventId=?;"
      val stmt = client.prepare(query)
      partition.foreach { rowKey =>
        client.execute(stmt.bind(rowKey.toString))
      }
    })
  }
}
