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

import scala.collection.mutable.Queue

class FiniteQueue[A] extends Queue[A] {

  /**
    * Enqueues with a finite size.
    * Dequeues according to the FIFO principle.
    * @param elem the element to add.
    * @param maxSize the maximum size.
    */
  def enqueueFinite(elem: A, maxSize: Int): Unit = {
    this.enqueue(elem)
    while (this.size > maxSize) {
      this.dequeue()
    }
  }

}
