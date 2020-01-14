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

import java.util.Properties

import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.codefeedr.plugins.ghtorrent.protocol.GHTorrent.Record
import org.codefeedr.plugins.ghtorrent.protocol.GitHub.Commit
import org.codefeedr.stages.TransformStage
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.JsonMethods.parse
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector
import org.codefeedr.buffer.serialization.Serializer

/** Transforms a Record into a [[Commit]].
  *
  * @param stageName the name of this stage.
  * @param sideOutput sideoutput configuration for unparse-able.
  */
class GHTCommitStage(stageName: String = "ght_commit",
                     sideOutput: SideOutput = SideOutput())
    extends TransformStage[Record, Commit](Some(stageName)) {

  // We only send to this topic once in a while, so we need to disable batches.
  val props = new Properties()
  props.put("bootstrap.servers", sideOutput.sideOutputKafkaServer)
  props.put("batch.size", "0")

  val outputTag = OutputTag[Record](sideOutput.sideOutputTopic)

  /** Transform from a Record to a Commit.
    *
    * @param source The input source with type [[Record]].
    * @return The transformed stream with type [[Commit]].
    */
  override def transform(source: DataStream[Record]): DataStream[Commit] = {
    val trans = source
      .filter(_.routingKey == "ent.commits.insert")
      .process(new CommitExtract(outputTag))

    // If side-output is enabled, unparseable instances are send to a Kafka topic.
    if (sideOutput.enabled) {
      trans
        .getSideOutput(outputTag)
        .addSink(
          new FlinkKafkaProducer[Record](
            sideOutput.sideOutputTopic,
            Serializer.getSerde[Record](Serializer.JSON4s),
            props))
    }

    trans
  }
}

/** Extract a Commit from a Record. **/
class CommitExtract(outputTag: OutputTag[Record])
    extends ProcessFunction[Record, Commit] {
  implicit lazy val defaultFormats = DefaultFormats ++ JavaTimeSerializers.all

  override def processElement(value: Record,
                              ctx: ProcessFunction[Record, Commit]#Context,
                              out: Collector[Commit]): Unit = {
    try {
      // Extract it into an optional.
      val parsedEvent = parse(value.contents).extractOpt[Commit]

      if (parsedEvent.isEmpty) {
        ctx.output(outputTag, value)
      } else {
        out.collect(parsedEvent.get)
      }
    } catch {
      case _: MappingException => ctx.output(outputTag, value)
    }
  }
}
