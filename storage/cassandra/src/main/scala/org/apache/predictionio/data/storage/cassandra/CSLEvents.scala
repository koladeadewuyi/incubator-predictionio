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

import com.datastax.driver.core._
import grizzled.slf4j.Logging
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.storage.LEvents
import org.apache.predictionio.data.storage.StorageClientConfig
import org.apache.predictionio.data.storage.cassandra.CSEventsUtil._
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

class CSLEvents(val client: Session, config: StorageClientConfig, val keySpace: String)
  extends LEvents with Logging {

  override
  def init(appId: Int, channelId: Option[Int] = None): Boolean = {

    logger.info("CS init")

    val tableName = getTableName(keySpace, appId, channelId)
    info(s"Auto-creating table $tableName if it doesn't already exist")
    val batchTableCreation =
      s"""
      |create table if not exists $tableName(
      | eventId text,
      | event text,
      | entityType text,
      | entityId text,
      | targetEntityType text,
      | targetEntityId text,
      | properties text,
      | prId text,
      | eventTime timestamp,
      | eventTimeZone text,
      | creationTime timestamp,
      | creationTimeZone text,
      | primary key (entityId, event, eventTime, entityType, eventId)
      |)
      |with clustering order by (event desc, eventTime desc, entityType desc);
      """.stripMargin
    val speedTableName = tableName + "_recent"
    val speedTableCreation =
    s"""
       |create table if not exists $tableName(
       |eventId text,
       |entityId text,
       |eventTime timestamp,
       |primary key (entityId)
       |)
       |with clustering order by (eventTime desc)
     """.stripMargin
    client.execute(batchTableCreation)
    true
  }

  override
  def remove(appId: Int, channelId: Option[Int] = None): Boolean = {

    logger.info("CS remove")
    try {
      val tableName = getTableName(keySpace, appId, channelId)
      info(s"Removing table $tableName if it exists")
      val query = s"drop table if exists $keySpace.$tableName"
      client.execute(query)
      true
    } catch {
      case e: Exception => {
        error(s"Fail to remove table for appId $appId. Exception: $e")
        false
      }
    }
  }

  override
  def close(): Unit = {
    client.close()
  }

  override
  def futureInsert(
    event: Event, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext):
    Future[String] = {

    val keyedEvent = eventToPut(event, appId)
    val rowKeyString = keyedEvent.eventid
    val preparedStmt = getInsertStatement(client, keySpace, appId, channelId, keyedEvent).get

    Future {
      val stmt = preparedStmt.bind()
      stmt.setString("eventId", keyedEvent.eventid)
      stmt.setString("event", keyedEvent.event)
      stmt.setString("entityType", keyedEvent.entitytype)
      stmt.setString("entityId", keyedEvent.entityid)
      stmt.setString("properties", keyedEvent.properties)
      keyedEvent.prid.map { x => stmt.setString("prId", x) }
      stmt.setTimestamp("eventTime", keyedEvent.eventtime.toDate)
      stmt.setString("eventTimeZone", keyedEvent.eventtimezone)
      stmt.setTimestamp("creationTime", keyedEvent.creationtime.toDate)
      stmt.setString("creationTimeZone", keyedEvent.creationtimezone)
      keyedEvent.targetentitytype.map { x => stmt.setString("targetEntityType", x) }
      keyedEvent.targetentityid.map { x => stmt.setString("targetEntityId", x) }

      client.execute(stmt)
      rowKeyString
    }
  }

  override
  def futureGet(
    eventId: String, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext):
    Future[Option[Event]] = {
    Future {
        val tableName = getTableName(keySpace, appId, channelId)
        val query = s"select * from $tableName where eventId = ? limit 1"
//        logger.info(s"$query")
        Option(client.execute(client.prepare(query).bind(tableName, eventId)).one).map {
          rs => resultToEvent(rs, appId)
        }
      }
    }

  override
  def futureDelete(
    eventId: String, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext):
    Future[Boolean] = {
    Future {
      val tableName = getTableName(keySpace, appId, channelId)
      val query = s"delete * from $tableName where eventId = ? if exists"
      Option(client.execute(client.prepare(query).bind(eventId)).one).exists {
        rs => rs.getBool("[applied]")
      }
    }
  }

  override
  def futureFind(
    appId: Int,
    channelId: Option[Int] = None,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    entityType: Option[String] = None,
    entityId: Option[String] = None,
    eventNames: Option[Seq[String]] = None,
    targetEntityType: Option[Option[String]] = None,
    targetEntityId: Option[Option[String]] = None,
    limit: Option[Int] = None,
    reversed: Option[Boolean] = None)(implicit ec: ExecutionContext):
    Future[Iterator[Event]] = {

    Future {
      require(!((reversed == Some(true)) && (entityType.isEmpty || entityId.isEmpty)),
        "the parameter reversed can only be used with both entityType and entityId specified.")

      val tableName = getTableName(keySpace, appId, channelId)
//      val startTimeClause = if (startTime.isDefined) s"eventTime >= ${startTime.getOrElse(new DateTime(0)).toDate}" else ""
//      val untilTimeClause = if (untilTime.isDefined) s"eventTime <= ${untilTime.getOrElse(new DateTime()).toDate}" else ""
//      val entityTypeClause = if (entityType.isDefined && entityType.get.nonEmpty) s"entityType = '${entityType.get}'" else ""
      val entityTypeClause = ""
      val entityIdClause = if (entityId.isDefined && entityId.get.nonEmpty) s"entityId = '${entityId.get}'" else ""
//      val eventNamesClause = if (eventNames.getOrElse(List.empty).nonEmpty) s"event in ('${eventNames.getOrElse(List.empty).mkString("','")}')" else ""
      val targetEntityTypeClause = if (targetEntityType.isDefined && targetEntityType.get.exists(_.nonEmpty)) s"targetEntityType = '${targetEntityType.flatten.get}'" else ""
      val targetEntityIdClause = if (targetEntityId.isDefined && targetEntityId.get.exists(_.nonEmpty)) s"targetEntityId = '${targetEntityId.flatten.get}'" else ""
//      val ordering = if (reversed == Some(true)) s"order by eventTime desc" else "" // Already ordered by eventTime in desc order
      val ordering = ""
      val limitClause = if (limit.isDefined) s"limit ${limit.get}" else ""

      val clauses = List(
//        startTimeClause,
//        untilTimeClause,
        entityTypeClause,
        entityIdClause,
//        eventNamesClause,
        targetEntityTypeClause,
        targetEntityIdClause
      ).filterNot(clause => clause.isEmpty)

      val query =
        s"""
           |select * from $tableName
            | ${if (clauses.isEmpty) "" else s" where ${clauses.mkString(" and ")}"}
            | $ordering
            | $limitClause
            | ${if (clauses.isEmpty || entityId == None) "" else "allow filtering" }
           """.stripMargin
//      logger.info(s"$query")

      val result = client.execute(query).all()
      val events = result.iterator().map { e =>
        resultToEvent(e, appId)
      }
      events
    }
  }

}
