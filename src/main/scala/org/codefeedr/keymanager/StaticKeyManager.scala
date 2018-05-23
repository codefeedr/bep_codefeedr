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
package org.codefeedr.keymanager

/**
  * Key manager implementation with a static set of keys. Does not allow for more than one key
  * per target, nor does it keep track of the number of uses.
  *
  * @param map Map of target -> key.
  */
class StaticKeyManager(map: Map[String, String] = Map()) extends KeyManager with Serializable {

  override def request(target: String, numberOfCalls: Int): Option[ManagedKey]= {
    if (target == null)
      throw new IllegalArgumentException()

    if (numberOfCalls == 0) {
      return Option.empty
    }

    val key = map.get(target)
    if (key.isEmpty)
      None
    else
      Some(ManagedKey(key.get, Int.MaxValue))
  }

}
