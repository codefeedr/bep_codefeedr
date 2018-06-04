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

import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Keeps track of all types of serde's and creates instances of serdes.
  */
object Serializer {

  /**
    * JSON serde support.
    * See: http://json4s.org/
    */
  val JSON = "JSON"

  /**
    * BSON serde support.
    * See: http://bsonspec.org/
    */
  val BSON = "BSON"

  /**
    * Kryo serde support.
    * https://github.com/EsotericSoftware/kryo
    */
  val KRYO = "KRYO"

  /**
    * Retrieve a serde.
    *
    * Default is JSONSerde.
    * @param name the name of the serde, see values above for the options.
    * @tparam T the type which has to be serialized/deserialized.
    * @return the serde instance.
    */
  def getSerde[T <: AnyRef : ClassTag : TypeTag](name: String)(implicit typeInfo : TypeInformation[T])= name match {
    case "JSON" => JSONSerde[T]
    case "BSON" => BsonSerde[T]
    case "KRYO" => KryoSerde[T]
    case _ => JSONSerde[T] //default is JSON
  }

}
