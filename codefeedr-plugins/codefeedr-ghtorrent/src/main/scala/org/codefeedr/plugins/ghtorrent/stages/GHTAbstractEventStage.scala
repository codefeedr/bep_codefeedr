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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag}
import org.codefeedr.plugins.ghtorrent.protocol.GHTorrent.{Event, Record}
import org.codefeedr.stages.TransformStage
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.JsonMethods.parse
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector
import org.codefeedr.buffer.serialization.Serializer

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

/** Configuration for a side-output if parsing fails.
  *
  * @param enabled if side-output is enabled.
  * @param sideOutputTopic the topic to side-output to.
  * @param sideOutputKafkaServer the broker of the output server.
  */
case class SideOutput(enabled: Boolean = true,
                      sideOutputTopic: String = "parse_exception",
                      sideOutputKafkaServer: String = "localhost:9092")

/** Transforms a GHTRecord to an [[Event]].
  *
  * @param stageName the name of this stage (must be unique per stage).
  * @param routingKey the routing_key to filter on.
  * @tparam T the type of this event.
  */
protected class GHTAbstractEventStage[
    T <: Serializable with AnyRef with Event: TypeTag: ClassTag: TypeInformation](
    stageName: String,
    routingKey: String,
    sideOutput: SideOutput = SideOutput())
    extends TransformStage[Record, T](Some(stageName)) {

  // We only send to this topic once in a while, so we need to disable batches.
  val props = new Properties()
  props.put("bootstrap.servers", sideOutput.sideOutputKafkaServer)
  props.put("batch.size", "0")

  val outputTag = OutputTag[Record](sideOutput.sideOutputTopic)

  /** Transforms and parses [[Event]] from [[Record]].
    *
    * @param source The input source with type Record.
    * @return The transformed stream with type T.
    */
  override def transform(source: DataStream[Record]): DataStream[T] = {
    val trans = source
      .process(new EventExtract[T](routingKey, outputTag))

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

/** Filters on routing key and extracts */
class EventExtract[T: Manifest](routingKey: String,
                                outputTag: OutputTag[Record])
    extends ProcessFunction[Record, T] {

  implicit lazy val defaultFormats = DefaultFormats ++ JavaTimeSerializers.all

  /** Filters on key and extracts correct event. */
  override def processElement(value: Record,
                              ctx: ProcessFunction[Record, T]#Context,
                              out: Collector[T]): Unit = {
    if (value.routingKey != routingKey) return //filter on routing keys

    try {
      // Extract it into an optional.
      val parsedEvent = parse(value.contents).extractOpt[T]

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
