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
package org.codefeedr.pipeline.buffer

import java.util.{Properties, UUID}

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.codefeedr.pipeline.{PipelineBuilder, StageAttributes}
import org.codefeedr.testUtils.{SimpleSourcePipelineObject, StringType}
import org.scalatest.{BeforeAndAfter, FunSuite}

class KafkaBufferTest extends FunSuite with BeforeAndAfter {

  var client : AdminClient = _
  var kafkaBuffer : KafkaBuffer[StringType] = _

  before {
    //set all the correct properties
    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    //connect with Kafka
    client = AdminClient.create(props)

    //setup simple kafkabuffer
    val pipeline = new PipelineBuilder().append(new SimpleSourcePipelineObject()).build()
    kafkaBuffer = new KafkaBuffer[StringType](pipeline, pipeline.bufferProperties, StageAttributes(),"test-subject")
  }


  test ("A topic not existing should be created") {
    val uuid = UUID.randomUUID().toString //random topic

    assert(!exists(uuid))
    kafkaBuffer.checkAndCreateSubject(uuid, "localhost:9092")
    assert(exists(uuid))
  }

  test ("A non-existent schema should throw an exception") {
    assertThrows[SchemaNotFoundException] {
      kafkaBuffer.getSchema("nOnExistent")
    }
  }

  test ("A schema should correctly be exposed") {
    assert(kafkaBuffer.exposeSchema())
  }

  /**
    * Check if topic exists.
    * @param topic topic to check.
    * @return if Kafka registered the topic.
    */
  def exists(topic : String) : Boolean = {
    client
      .listTopics()
      .names()
      .get()
      .contains(topic)
  }

}
