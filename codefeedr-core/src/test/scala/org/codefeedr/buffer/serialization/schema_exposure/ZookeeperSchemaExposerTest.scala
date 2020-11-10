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
package org.codefeedr.buffer.serialization.schema_exposure

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.BeforeAndAfterAll

class ZookeeperSchemaExposerTest
    extends SchemaExposerTest
    with EmbeddedKafka
    with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    implicit val config = EmbeddedKafkaConfig(zooKeeperPort = 2181)
    EmbeddedKafka.start()
    println("Just started embedded kafka.")
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  override def getSchemaExposer(): SchemaExposer =
    new ZookeeperSchemaExposer("localhost:2181")
}
