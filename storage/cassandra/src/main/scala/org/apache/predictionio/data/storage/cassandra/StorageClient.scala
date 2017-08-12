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

import org.apache.predictionio.data.storage.BaseStorageClient
import org.apache.predictionio.data.storage.StorageClientConfig
import com.datastax.driver.core._
import java.net.URI

import grizzled.slf4j.Logging


case class CassandraConnectionUri(connectionString: String) {

  private val uri = new URI(connectionString)

  private val additionalHosts = Option(uri.getQuery) match {
    case Some(query) => query.split('&').map(_.split('=')).filter(param => param(0) == "host").map(param => param(1)).toSeq
    case None => Seq.empty
  }

  val host = uri.getHost
  val hosts = Seq(uri.getHost) ++ additionalHosts
  val port = uri.getPort
  val keySpace = uri.getPath.substring(1)

}

class StorageClient(val config: StorageClientConfig)
  extends BaseStorageClient with Logging {

  val hosts: Seq[String] = config.properties.getOrElse("HOSTS", "127.0.0.1").split(',').toSeq
  val keySpace = "pio_event"

  info(s"cassandra hosts $hosts")

  private def createSessionAndInitKeyspace(uri: CassandraConnectionUri,
                                             defaultConsistencyLevel: ConsistencyLevel = QueryOptions.DEFAULT_CONSISTENCY_LEVEL) = {
    val cluster = new Cluster.Builder().
      addContactPoints(uri.hosts.toArray: _*).
      withPort(uri.port).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build

    val session = cluster.connect
    try {
      info(s"Auto-creating keyspace $keySpace if it doesn't already exist")
      session.execute(s"Create keyspace if not exists $keySpace with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }")
      session.execute(s"use $keySpace")
      info(s"Using keyspace $keySpace")
    } catch {
      case e: Exception => error(s"$e")
    }
    session
  }

  val client = {
    val uri = CassandraConnectionUri(s"cassandra://localhost:9042/$keySpace")
    val session = createSessionAndInitKeyspace(uri)
    session
  }

  override
  val prefix = "CS"
}
