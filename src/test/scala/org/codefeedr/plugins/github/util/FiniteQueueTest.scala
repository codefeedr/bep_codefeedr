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
package org.codefeedr.plugins.github.util

import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable.Queue

class FiniteQueueTest extends FunSuite with BeforeAndAfter {

  var queue : FiniteQueue[String] = new FiniteQueue[String]()

  before {
    queue = new FiniteQueue[String]()
  }

  test ("Should properly enqueue") {
    queue.enqueueFinite("test", 2)
    queue.enqueueFinite("test2", 2)

    assert(queue.contains("test"))
    assert(queue.contains("test2"))
  }

  test ("Should properly remove on enqueue") {
    queue.enqueueFinite("test", 2)
    queue.enqueueFinite("test2", 2)
    queue.enqueueFinite("test3", 2)

    assert(queue.contains("test2"))
    assert(queue.contains("test3"))
    assert(!queue.contains("test"))
  }


}
