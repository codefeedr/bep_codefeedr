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
package org.codefeedr.plugins.github.stages

import java.io.InputStream
import java.util

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.{InputStage, PipelineBuilder}
import org.codefeedr.plugins.github.GitHubProtocol.{Event, PushEvent}
import org.codefeedr.plugins.github.requests.EventService
import org.scalatest.FunSuite
import org.apache.flink.api.scala._

import scala.io.Source

class GitHubEventToPushEventTest extends FunSuite {

  test ("GitHubEventToPushEvent integration test") {
    val pipeLine = new PipelineBuilder()
      .append(new SimpleEventSource)
      .append(new GitHubEventToPushEvent())
      .append { x : DataStream[PushEvent] =>
        x.addSink(new PushEventCollectSink)
      }
      .build()
      .startMock()

    //+- there is only 1 push event
    assert(PushEventCollectSink.result.size == 1)
  }
}

class SimpleEventSource extends InputStage[Event] {

  val stream : InputStream = getClass.getResourceAsStream("/sample_events.json")
  val sampleEvents : String = Source.fromInputStream(stream).getLines.mkString

  override def main(): DataStream[Event] = {
    val events = new EventService(false, null)
      .parseEvents(sampleEvents)

    pipeline.environment.fromCollection(events)
  }
}
object PushEventCollectSink {
  val result = new util.ArrayList[PushEvent]() //mutable list
}

class PushEventCollectSink extends SinkFunction[PushEvent] {

  override def invoke(value: PushEvent): Unit = {
    synchronized {
      PushEventCollectSink.result.add(value)
    }
  }

}
