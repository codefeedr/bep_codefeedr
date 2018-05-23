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

package org.codefeedr.plugins.elasticsearch

import org.codefeedr.testUtils.StringType
import org.scalatest.FunSuite

class ElasticSearchOutputStageTest extends FunSuite {
  val index = "testIndex"

  test("Should create config object") {
    val stage = new ElasticSearchOutputStage[StringType](index)

    val config = stage.createConfig()
    assert(config != null)
  }

  test("Should add localhost to connections if none are given") {
    val stage = new ElasticSearchOutputStage[StringType](index)

    val addresses = stage.createTransportAddresses()

    assert(addresses.size() == 1)
    assert(addresses.get(0).getHostName == "localhost")
    assert(addresses.get(0).getPort == 9300)
  }

  test("Should add configured hosts") {
    val servers = Set("es://example.com:9300", "es://google.com:9200")
    val stage = new ElasticSearchOutputStage[StringType](index, servers)

    val addresses = stage.createTransportAddresses()

    assert(addresses.size() == 2)
    assert(addresses.get(0).getHostName == "example.com")
    assert(addresses.get(0).getPort == 9300)
    assert(addresses.get(1).getHostName == "google.com")
    assert(addresses.get(1).getPort == 9200)
  }

  test("Should not add servers with no es schema") {
    val servers = Set("es://example.com:9300", "myHost:9200")
    val stage = new ElasticSearchOutputStage[StringType](index, servers)

    val addresses = stage.createTransportAddresses()

    assert(addresses.size() == 1)
  }

}
