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
package org.codefeedr.buffer.serialization

import org.scalatest.FunSuite
import org.codefeedr.api._

class SerializerTest extends FunSuite {

  case class Item()

  test("Should recognise JSON") {
    val serde = Serializer.getSerde[Item](Serializer.JSON4s)

    assert(serde.isInstanceOf[JSON4sSerde[Item]])
  }

  test("Should recognise Bson") {
    val serde = Serializer.getSerde[Item](Serializer.BSON)

    assert(serde.isInstanceOf[BsonSerde[Item]])
  }

  test("Should recognise Kryo") {
    val serde = Serializer.getSerde[Item](Serializer.KRYO)

    assert(serde.isInstanceOf[KryoSerde[Item]])
  }

  test("Should default to JSON") {
    val serde = Serializer.getSerde[Item]("YAML")

    assert(serde.isInstanceOf[JSON4sSerde[Item]])
  }

  test("I should be able to add my own serializer") {
    Serializer.register[JSON4sSerde[_]]("my_very_own_serializer")

    val serde = Serializer.getSerde[Item]("my_very_own_serializer")
    assert(serde.isInstanceOf[JSON4sSerde[Item]])
  }

  test("I should be able to add my own serializer through codefeedr entrypoint") {
    registerSerializer[JSON4sSerde[_]]("wow")

    val serde = Serializer.getSerde[Item]("wow")
    assert(serde.isInstanceOf[JSON4sSerde[Item]])
  }

  test("Cannot register duplicate names.") {
    assertThrows[IllegalArgumentException] {
      Serializer.register[JSON4sSerde[_]]("BSON")
    }

    Serializer.register[BsonSerde[_]]("very_unique")

    assertThrows[IllegalArgumentException] {
      Serializer.register[JSON4sSerde[_]]("very_unique")
    }
  }
}
