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
package org.codefeedr.stages.kafka

import java.util.Properties

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.codefeedr.buffer.serialization.Serializer
import org.codefeedr.stages.OutputStage

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** KafkaOutput stage, which sends to a Kafka topic.
  *
  * @param topic The topic to send to.
  * @param properties Kafka properties, see https://kafka.apache.org/documentation/#consumerconfigs
  * @param serializer The serializer to use for serialization of the data, see [[Serializer]].
  * @tparam T Type of data in Kafka.
  */
class KafkaOutput[T <: Serializable with AnyRef: ClassTag: TypeTag](
    topic: String,
    properties: Properties,
    serializer: String = Serializer.JSON4s,
    stageId: Option[String] = None)
    extends OutputStage[T](stageId) {

  //get correct serde, will fallback to JSON
  private val serde = Serializer.getSerde[T](serializer)

  //add producer as sink
  override def main(source: DataStream[T]): Unit = {
    source.addSink(new FlinkKafkaProducer[T](topic, serde, properties))
  }
}
