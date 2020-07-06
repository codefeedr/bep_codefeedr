/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.codefeedr.plugins.ghtorrent.stages

import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.Context
import org.codefeedr.plugins.ghtorrent.protocol.GHTorrent.Record
import org.codefeedr.plugins.ghtorrent.util.GHTorrentRabbitMQSource
import org.codefeedr.stages.InputStage
import org.apache.flink.api.scala._

/** GHTorrent input stage following the GHTorrent Streaming specification.
  *
  * @param username your username to specify to GHTorrent.
  * @param stageName name of this stage, "ghtorrent_input" is default.
  * @param host the host name, default is localhost.
  * @param port the port, default is 5672.
  * @param routingKeysFile the location of the routingKeysFile (in the resources directory).
  * @param virtualHost the virtual host of GHTorrent.
  * @param password your password to specify to GHTorrent.
  */
class GHTInputStage(username: String,
                    stageName: String = "ght_input",
                    host: String = "localhost",
                    port: Int = 5672,
                    routingKeysFile: String = "routing_keys.txt",
                    virtualHost: String = "/",
                    password: String = "streamer")
    extends InputStage[Record](Some(stageName)) {

  /** Transforms GHTorrent data to a Record format.
    *
    * @param context the CodeFeedr context.
    * @return A newly created DataStream of Records.
    */
  override def main(context: Context): DataStream[Record] = {
    context.env
      .addSource(
        new GHTorrentRabbitMQSource(username,
                                    host,
                                    port,
                                    routingKeysFile,
                                    password = password,
                                    virtualHost = virtualHost))
      .map { x =>
        val splitEl = x.split("#", 2)
        val routingKey = splitEl(0)
        val record = splitEl(1)

        Record(routingKey, record)
      }
  }
}
