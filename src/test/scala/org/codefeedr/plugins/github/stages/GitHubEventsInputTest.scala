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

import java.util

import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.PipelineBuilder
import org.codefeedr.pipeline.buffer.{BufferType, KafkaBuffer}
import org.codefeedr.plugins.github.GitHubProtocol.Event
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.codefeedr.plugins.StringType
import org.scalatest.{BeforeAndAfter, FunSuite}

//integration test
class GitHubEventsInputTest extends FunSuite {


  test("A GitHubEventsInputTest should create the proper results") {
    val pipeLine = new PipelineBuilder()
      .append(new GitHubEventsInput(0, 1000))
      .append { x : DataStream[Event] =>
        x.addSink(new EventCollectSink)
      }
      .build()
      .startMock()

    //+- no requests, so no result
    assert(EventCollectSink.result.size == 0)
  }

}

object EventCollectSink {
  val result = new util.ArrayList[Event]() //mutable list
}

class EventCollectSink extends SinkFunction[Event] {

  override def invoke(value: Event): Unit = {
    synchronized {
      EventCollectSink.result.add(value)
    }
  }

}
