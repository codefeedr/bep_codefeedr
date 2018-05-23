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
package org.codefeedr.plugins.github.events

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.codefeedr.keymanager.KeyManager
import org.codefeedr.plugins.github.GitHubProtocol.Event
import org.codefeedr.plugins.github.requests.EventService

/**
  * Creates a new (Flink) GitHub event source.
  * NOTE: This source will always run in parallelism = 1.
  * @param numOfPolls the amount of polls to do before cancelling.
  * @param waitTime the wait time between each poll (in ms).
  * @param keyManager the key manager to use for the requests.
  * @param duplicateFilter enable to filter duplicates.
  * @param duplicateCheckSize the amounts of id's to cache for duplicates.
  */
class EventSource(numOfPolls : Int,
                  waitTime : Int,
                  keyManager : KeyManager,
                  duplicateFilter: Boolean,
                  duplicateCheckSize : Int) extends RichSourceFunction[Event] {

  //check if the source should be unbounded.
  def isUnbounded = numOfPolls == -1

  var numOfPollsRemaining = numOfPolls
  var isRunning = false
  var eventService : EventService = null

  /**
    * Opens and initializes this source.
    * @param parameters the configuration parameters.
    */
  override def open(parameters: Configuration): Unit = {
    isRunning = true
    eventService = new EventService(duplicateFilter, keyManager, duplicateCheckSize)
  }

  /**
    * Closes the sources.
    */
  override def cancel(): Unit = {
    isRunning = false
    eventService = null
  }

  /**
    * Runs the source.
    * @param ctx the context to collect the elements to.
    */
  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    while (isRunning && numOfPollsRemaining != 0) {
      if (!isUnbounded) {
        numOfPollsRemaining -= 1
      }

      eventService
        .getLatestEvents()
        .foreach(ctx.collect)

      Thread.sleep(waitTime)
    }

    cancel()
  }


}
