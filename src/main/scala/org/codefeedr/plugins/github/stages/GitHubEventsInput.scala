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

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.InputStage
import org.codefeedr.plugins.github.GitHubProtocol.Event
import org.codefeedr.plugins.github.events.EventSource
import org.codefeedr.plugins.github.requests.EventService
import org.apache.flink.api.scala._

class GitHubEventsInput(numOfPolls : Int = -1) extends InputStage[Event] {

  override def main(): DataStream[Event] = {
    pipeline.environment.addSource(new EventSource(numOfPolls, 1000, pipeline.keyManager))
  }
}