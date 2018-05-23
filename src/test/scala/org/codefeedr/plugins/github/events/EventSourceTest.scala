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
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.keymanager.StaticKeyManager
import org.codefeedr.plugins.github.GitHubProtocol.Event
import org.codefeedr.plugins.github.requests.EventService
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._

class EventSourceTest extends FunSuite with MockitoSugar {

  test ("The event service should be properly stopped.") {
    val mockContext = mock[SourceFunction.SourceContext[Event]]
    val mockService = mock[EventService]
    val mockEvent = mock[Event]

    val eventSource = spy(new EventSource(2, 0, new StaticKeyManager(), true, 3))
    eventSource.open(new Configuration())
    eventSource.eventService = mockService

    //2 mock events
    when(mockService.getLatestEvents()).thenReturn(List(mockEvent, mockEvent))

    assert(!eventSource.isUnbounded)
    assert(eventSource.numOfPollsRemaining == 2)

    eventSource.run(mockContext)

    assert(eventSource.numOfPollsRemaining == 0)
    verify(eventSource).cancel()
    verify(mockContext, times(4)).collect(any[Event]) // 2 runs * 2 events = 4 calls
  }

  test ("The eventsource should be unbounded if configured") {
    val mockContext = mock[SourceFunction.SourceContext[Event]]
    val mockService = mock[EventService]
    val mockEvent = mock[Event]

    val eventSource = spy(new EventSource(-1, 500, new StaticKeyManager(), true, 3))
    eventSource.open(new Configuration())
    eventSource.eventService = mockService

    //2 mock events
    when(mockService.getLatestEvents()).thenReturn(List(mockEvent, mockEvent))
    assert(eventSource.isUnbounded)

    //stop event source loop
    new Thread {
      override def run: Unit = {
        Thread.sleep(1000)
        eventSource.isRunning = false
      }
    }.start()

    eventSource.run(mockContext)
    verify(eventSource).cancel()
    verify(mockContext, atLeast(1)).collect(any[Event])
  }



}
