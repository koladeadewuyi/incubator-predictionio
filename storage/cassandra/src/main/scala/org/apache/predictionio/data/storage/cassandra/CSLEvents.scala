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

    val tableName = getTableName(keySpace, appId, channelId)
    info(s"Auto-creating table $tableName if it doesn't already exist")
    val query =
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
      | primary key (entityId, event, eventId, eventTime, entityType)
      |)
      """.stripMargin
    client.execute(query)
    true
  }

  override
  def remove(appId: Int, channelId: Option[Int] = None): Boolean = {
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
    val rowKeyString = keyedEvent.eventId
    val preparedStmt = getInsertStatement(client, keySpace, appId, channelId, keyedEvent).get

    Future {
      val stmt = preparedStmt.bind()
      stmt.setString("eventId", keyedEvent.eventId)
      stmt.setString("event", keyedEvent.event)
      stmt.setString("entityType", keyedEvent.entityType)
      stmt.setString("entityId", keyedEvent.entityId)
      stmt.setString("properties", keyedEvent.properties)
      keyedEvent.prId.map { x => stmt.setString("prId", x) }
      stmt.setTimestamp("eventTime", keyedEvent.eventTime.toDate)
      stmt.setString("eventTimeZone", keyedEvent.eventTimeZone)
      stmt.setTimestamp("creationTime", keyedEvent.creationTime.toDate)
      stmt.setString("creationTimeZone", keyedEvent.creationTimeZone)
      keyedEvent.targetEntityType.map { x => stmt.setString("targetEntityType", x) }
      keyedEvent.targetEntityId.map { x => stmt.setString("targetEntityId", x) }

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
      val startTimeClause = if (startTime.isDefined) s"eventTime >= ${startTime.map(_.getMillis).getOrElse(0)}" else ""
      val untilTimeClause = if (untilTime.isDefined) s"eventTime <= ${untilTime.map(_.getMillis).getOrElse(Long.MaxValue)}" else ""
      val entityTypeClause = if (entityType.isDefined && entityType.get.nonEmpty) s"entityType = '${entityType.get}'" else ""
      val entityIdClause = if (entityId.isDefined && entityId.get.nonEmpty) s"entityId = '${entityId.get}'" else ""
      val eventNamesClause = if (eventNames.getOrElse(List.empty).nonEmpty) s"eventNames in ('${eventNames.getOrElse(List.empty).mkString("','")}')" else ""
      val targetEntityTypeClause = if (targetEntityType.isDefined && targetEntityType.get.exists(_.nonEmpty)) s"targetEntityType = '${targetEntityType.flatten.get}'" else ""
      val targetEntityIdClause = if (targetEntityId.isDefined && targetEntityId.get.exists(_.nonEmpty)) s"targetEntityId = '${targetEntityId.flatten.get}'" else ""
      val ordering = if (reversed == Some(true)) s"order by eventTime desc" else ""
      val limitClause = if (limit.isDefined) s"limit ${limit.get}" else ""

      val clauses = List(
        startTimeClause,
        untilTimeClause,
        entityTypeClause,
        entityIdClause,
        eventNamesClause,
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
      val result = client.execute(query).all()
      val events = result.iterator().map { e =>
        resultToEvent(e, appId)
      }
      events
    }
  }

}
