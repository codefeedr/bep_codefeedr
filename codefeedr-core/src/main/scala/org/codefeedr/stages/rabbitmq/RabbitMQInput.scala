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

package org.codefeedr.stages.rabbitmq

import com.sun.tools.javac.code.TypeTag
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.buffer.serialization.AvroSerde
import org.codefeedr.pipeline.PipelineItem
import org.codefeedr.stages.{InputStage, StageAttributes}

import scala.reflect.{ClassTag, Manifest}

/**
  * Input stage pulling data from a RabbitMQ queue.
  *
  * @param stageAttributes Optional attributes
  * @tparam T Type of value to pull from the queue
  */
class RabbitMQInput[T <: PipelineItem : ClassTag : TypeTag : AvroSerde](stageAttributes: StageAttributes = StageAttributes())
  extends InputStage[T](stageAttributes) {

  override def main(): DataStream[T] = {
    null
  }
}
