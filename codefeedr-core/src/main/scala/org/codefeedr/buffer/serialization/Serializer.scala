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

import scala.reflect.ClassTag
import scala.reflect._
import scala.reflect.runtime.universe._

/** Keeps track of all types of SerDes and creates instances. */
object Serializer extends Enumeration {

  type SerializerType = String

  /** JSON serde support.
    * See: http://json4s.org/
    */
  val JSON4s = "JSON4s"

  /** BSON serde support.
    * See: http://bsonspec.org/
    */
  val BSON = "BSON"

  /** Kryo serde support.
    * https://github.com/EsotericSoftware/kryo
    */
  val KRYO = "KRYO"

  /** Reserved key words for serializer names. */
  private val reserved = List(JSON4s, BSON, KRYO)

  /** Map containing (type) references to SerDe by name. */
  private var registry: Map[String, Manifest[_ <: AbstractSerde[_]]] = Map()

  /** Retrieve a serde.
    *
    * Default is JSONSerde.
    * @param name the name of the serde, see values above for the options.
    * @tparam T the type which has to be serialized/deserialized.
    * @return the serde instance.
    */
  def getSerde[T <: Serializable with AnyRef: ClassTag: TypeTag](name: String) =
    name match {
      case "JSON4s" => JSON4sSerde[T]
      case "BSON"   => BsonSerde[T]
      case "KRYO"   => KryoSerde[T]
      case _ if registry.exists(_._1 == name) => {
        val tt = typeTag[T]
        val ct = classTag[T]

        registry
          .get(name)
          .get
          .runtimeClass
          .getConstructors()(0) // Get constructor of runtime class.
          .newInstance(tt, ct) // Provide class and type tags.
          .asInstanceOf[AbstractSerde[T]] // Create instance of serde.
      }
      case _ => JSON4sSerde[T] //default is JSON
    }

  /** Registers a new SerDe. This SerDe needs to be subclassed from [[AbstractSerde]].
    *
    * In order to register your own SerDe:
    * 1. Create one by extending [[AbstractSerde]]:
    * {{{
    * class YourSerde[T <: Serializable with AnyRef: TypeTag: ClassTag]
    *     extends AbstractSerde[T]
    * }}}
    * 2. Register your SerDe:
    * {{{
    * Serializer.register[YourSerde[_]]("my_serde")
    * }}}
    * 3. In the pipeline select your SerDe:
    * {{{
    *   pipelineBuilder.setBufferProperty(Buffer.SERIALZER, "my_serde")
    * }}}
    *
    * @param name Name of the SerDe. This needs to be unique. Reserved keywords are: JSON, BSON, KRYO.
    * @param ev Implicit Manifest of the class.
    * @tparam T Type of the serializer.
    * @throws IllegalArgumentException Thrown when name is not unique/already registered.
    */
  def register[T <: AbstractSerde[_]](name: String)(
      implicit ev: Manifest[T]) = {
    if (reserved.contains(name) || registry.exists(_._1 == name))
      throw new IllegalArgumentException("Serializer (name) already exists.")

    // Add manifest to map.
    registry += (name -> ev)
  }

}
