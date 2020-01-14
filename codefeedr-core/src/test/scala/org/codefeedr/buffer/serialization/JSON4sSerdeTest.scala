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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.scalatest.{BeforeAndAfter, FunSuite}

private case class SimpleCaseClass(str: String, i: Int)

class JSON4sSerdeTest extends FunSuite with BeforeAndAfter {

  private var serde: JSON4sSerde[SimpleCaseClass] = null

  before {
    serde = new JSON4sSerde[SimpleCaseClass]()
  }

  test("Serializes simple values") {
    val value = SimpleCaseClass("hello", 42)

    val byteArray = serde.serialize(value)

    val expected = """{"str":"hello","i":42}"""

    assert(byteArray sameElements expected.getBytes)
  }

  test("Simple case class equality") {
    val a = SimpleCaseClass("hello", 42)
    val b = SimpleCaseClass("hello", 42)

    assert(a == b)
  }

  test("Deserializes simple values") {
    val value = """{"str":"hello","i":42}"""
    val deserialized = serde.deserialize(value.getBytes)
    val expected = SimpleCaseClass("hello", 42)

    assert(expected == deserialized)
  }

  test("Deserializes simple serialized values") {
    val value = SimpleCaseClass("hello", 42)

    val serialized = serde.serialize(value)
    val deserialized = serde.deserialize(serialized)

    assert(deserialized == value)
  }

  test("Simple typeinformation check") {
    val typeInformation = TypeInformation.of(classOf[SimpleCaseClass])

    assert(typeInformation == serde.getProducedType)
  }

}
