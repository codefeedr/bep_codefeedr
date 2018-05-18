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
package org.codefeedr.pipeline.buffer

import com.sksamuel.avro4s.FromRecord
import org.codefeedr.pipeline.{Pipeline, PipelineItem, PipelineObject}

import scala.reflect.Manifest

/**
  * Buffer creator.
  *
  * Creates a buffer that reflects the sink of given object.
  *
  * If an in-buffer is needed: sinkObject should be the parent
  * If an out-buffer is needed: sinkObject should be the actual node that needs a sink
  *
  * @param pipeline Pipeline
  * @param sinkObject Object that writes to the buffer
  */
class BufferFactory[U <: PipelineItem, V <: PipelineItem, X <: PipelineItem, Y <: PipelineItem](pipeline: Pipeline, stage: PipelineObject[X, Y], sinkObject: PipelineObject[U, V]) {

  /**
    * Create a new buffer
    *
    * @tparam T Object type within the buffer
    * @return Buffer
    * @throws IllegalArgumentException When sinkObject is null
    * @throws IllegalStateException When buffer could not be instantiated due to bad configuration
    */
  def create[T <: AnyRef : Manifest : FromRecord](): Buffer[T] = {
    if (sinkObject == null) {
      throw new IllegalArgumentException("Buffer factory requires a sink object to determine buffer location")
    }

    val subject = sinkObject.getSinkSubject
    
    pipeline.bufferType match {
      case BufferType.None =>
        throw new IllegalStateException("Cannot instantiate buffer of type 'None'")
      case BufferType.Kafka =>
        new KafkaBuffer[T](pipeline, pipeline.bufferProperties, stage.attributes, subject)
    }
  }
}