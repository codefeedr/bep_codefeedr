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
package org.codefeedr.buffer

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.buffer.serialization.{AbstractSerde, Serializer}
import org.codefeedr.pipeline.Pipeline

import scala.reflect.runtime.universe._
import scala.reflect.{ClassTag, classTag}

/** A buffer is used in between stages.
  * It is often an external source that stores and queues elements (like Kafka).
  *
  * @param pipeline The pipeline for which we use this Buffer.
  * @param properties The properties of this buffer.
  * @param relatedStageName The name of the related stage the buffer is linked to. We need this to for instance set the topic id.
  * @tparam T Type of this buffer.
  */
abstract class Buffer[T <: Serializable with AnyRef: ClassTag: TypeTag](
    pipeline: Pipeline,
    properties: org.codefeedr.Properties,
    relatedStageName: String) {

  //Get type of the class at run time.
  val inputClassType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  //Get TypeInformation of generic (case) class.
  implicit val typeInfo = TypeInformation.of(inputClassType)

  /** Get the source for this buffer. A stage reads from this.
    *
    * @return The DataStream retrieved from a Buffer.
    */
  def getSource: DataStream[T]

  /** Get the sink function for this buffer. A stage writes to this.
    *
    * @return The SinkFunction retrieved from a Buffer.
    */
  def getSink: SinkFunction[T]

  /** Get serializer/deserializer of elements for a buffer.
    * Defaults to JSON.
    *
    * @return The configured serializer.
    */
  def getSerializer: AbstractSerde[T] = {
    val serializer =
      properties
        .getOrElse[String](Buffer.SERIALIZER, Serializer.JSON4s) //default is JSON

    Serializer.getSerde[T](serializer)
  }

  /** Returns the properties of this buffer. */
  def getProperties = properties
}

/**
  * Static features of a buffer.
  */
object Buffer {

  /** Serializer of the buffer (e.g. JSON). */
  val SERIALIZER = "SERIALIZER"
}
