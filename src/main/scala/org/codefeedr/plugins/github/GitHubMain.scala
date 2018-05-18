package org.codefeedr.plugins.github

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import com.sksamuel.avro4s.FromRecord
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.codefeedr.keymanager.redis.RedisKeyManager
import org.codefeedr.pipeline.buffer.serialization.{AvroSerde, Serializer}
import org.codefeedr.pipeline.{PipelineBuilder, PipelineItem}
import org.codefeedr.pipeline.buffer.{BufferType, KafkaBuffer}
import org.codefeedr.plugins.github.GitHubProtocol.{Event, PushEvent, PushPayload}
import org.codefeedr.plugins.github.requests.EventService
import org.codefeedr.plugins.github.stages.{GitHubEventToPushEvent, GitHubEventsInput, PrintJsonOutputStage}
import shapeless.datatype.avro.AvroType

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
/**
  * Just a test class to play around.
 */
object GitHubMain {

  def main(args : Array[String]) = {
    new PipelineBuilder()
      .setBufferType(BufferType.Kafka)
      .setBufferProperty(KafkaBuffer.SERIALIZER, Serializer.JSON)
      .append(new GitHubEventsInput(1))
      .append(new GitHubEventToPushEvent)
      .append(new PrintJsonOutputStage[PushEvent])
      .build()
      .startLocal()
  }

}
