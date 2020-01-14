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
 */
package org.codefeedr.plugins.rabbitmq.stages

import java.net.URI

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig
import org.codefeedr.buffer.serialization.{AbstractSerde, Serializer}
import org.codefeedr.plugins.rabbitmq.RMQSinkDurable
import org.codefeedr.stages.OutputStage

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Output stage for sending data to a RabbitMQ queue.
  *
  * @param queue The queue id to read from.
  * @param server The server connection string.
  * @param stageAttributes Attributes of this stage.
  * @tparam T Type of value to pull from the queue.
  */
class RabbitMQOutput[T <: Serializable with AnyRef: ClassTag: TypeTag](
    queue: String,
    stageName: String = "rmq_output",
    server: URI = new URI("amqp://localhost:5672"),
    stageId: Option[String] = None)
    extends OutputStage[T](Some(stageName)) {

  /** Add the InputSource to the Flink environment. */
  override def main(source: DataStream[T]): Unit = {
    val config = new RMQConnectionConfig.Builder()
      .setUri(server.toString)
      .build

    // Use a durable sink to match the RabbitMQInput version
    source.addSink(new RMQSinkDurable[T](config, queue, getSerializer))
  }

  /** The serializer to use for sending data to RabbitMQ.
    * Override to use a different serialization then JSON.
    *
    * @return The correct SerDe.
    */
  protected def getSerializer: AbstractSerde[T] = {
    val serializer = Serializer.JSON4s

    Serializer.getSerde[T](serializer)
  }
}
