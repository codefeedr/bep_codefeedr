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
package org.codefeedr.buffer.serialization

import java.nio.charset.StandardCharsets

import org.json4s.NoTypeHints
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.Serialization

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** JSON (de-)serializer
  *
  * @tparam T Type of the SerDe.
  */
class JSON4sSerde[T <: Serializable with AnyRef: TypeTag: ClassTag]
    extends AbstractSerde[T] {

  // Implicitly and lazily define the serialization to JSON.
  implicit lazy val formats = Serialization.formats(NoTypeHints) ++ JavaTimeSerializers.all

  /** Serializes a (generic) element into a json format.
    *
    * @param element The element to serialized.
    * @return A serialized byte array.
    */
  override def serialize(element: T): Array[Byte] = {
    val string = Serialization.write(element)(formats)
    string.getBytes(StandardCharsets.UTF_8)
  }

  /** Deserializes a (JSON) message into a (generic) case class
    *
    * @param message The message to deserialized.
    * @return A deserialized case class.
    */
  override def deserialize(message: Array[Byte]): T = {
    Serialization.read[T](new String(message, StandardCharsets.UTF_8))
  }
}

/** Companion object to simply instantiation of a JSONSerde. */
object JSON4sSerde {

  /** Creates new JSON Serde. */
  def apply[T <: Serializable with AnyRef: ClassTag: TypeTag]: JSON4sSerde[T] =
    new JSON4sSerde[T]()
}
