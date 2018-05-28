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
package org.codefeedr.stages.utilities

import org.scalatest.{BeforeAndAfter, FunSuite}

class DuplicateFilterTest extends FunSuite with BeforeAndAfter {

  var filter : DuplicateFilter[String] = null

  before {
    filter = new DuplicateFilter[String](3)
  }

  test("New items should all be returned") {
    val result = filter.check(List("1", "2", "3"))

    assert(result.size == 3)
    assert(result == List("1", "2", "3"))
  }

  test("Duplicate items should not be returned") {
    val result = filter.check(List("1", "2", "2"))

    assert(result.size == 2)
    assert(result == List("1", "2"))
  }

  test("Duplicate items should not be returned, if already in the queue") {
    filter.check(List("1", "2"))
    val result = filter.check(List("2", "2"))

    assert(result.size == 0)
  }

  test("If queue is full, FIFO should be used") {
    val firstResult = filter.check(List("1", "2", "3", "4")) //1 is removed from queue
    val result = filter.check(List("1", "2"))

    assert(firstResult.size == 4)
    assert(result.size == 1)
    assert(result.head == "1")
  }

}
