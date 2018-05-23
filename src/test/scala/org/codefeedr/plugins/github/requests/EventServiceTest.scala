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
package org.codefeedr.plugins.github.requests

import java.io.InputStream

import org.codefeedr.keymanager.StaticKeyManager
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.mockito.Mockito._
import org.mockito.Matchers.any

import scala.io.Source

class EventServiceTest extends FunSuite with BeforeAndAfter with MockitoSugar {

  val stream : InputStream = getClass.getResourceAsStream("/sample_events.json")
  val sampleEvents : String = Source.fromInputStream(stream).getLines.mkString
  var service : EventService = _

  before {
    service = new EventService(false, new StaticKeyManager())
  }

  test("A key should be properly set.") {
    service.setKey("12345")

    val header = Header("Authorization", Array("token 12345"))
    val headerSet = service.requestHeaders.find(_.key == "Authorization")
    assert(!headerSet.isEmpty)
    assert(headerSet.get.value.contains("token 12345"))
  }

  test("A header should be properly added") {
    val header = Header("example", Array("test"))

    service.updateOrAddHeader(header)

    val headerSet = service.requestHeaders.find(_.key == "example")
    assert(!headerSet.isEmpty)
    assert(headerSet.get.value.contains("test"))
  }

  test("A header should be properly added (twice)") {
    val header = Header("example", Array("test"))
    val header2 = Header("example", Array("test2"))

    service.updateOrAddHeader(header)
    service.updateOrAddHeader(header2)

    val headerSet = service.requestHeaders.find(_.key == "example")
    assert(!headerSet.isEmpty)
    assert(headerSet.get.value.contains("test2"))
  }

  test("Request headers should be properly propagated") {
    service.updateRequestHeaders(List(Header("ETag", Array("123"))))

    val headerSet = service.requestHeaders.find(_.key == "If-None-Match")
    assert(!headerSet.isEmpty)
    assert(headerSet.get.value.contains("123"))
  }

  test("Request headers should (not) be propagated") {
    service.updateRequestHeaders(List(Header("NotETag", Array("123"))))

    val headerSet = service.requestHeaders.find(_.key == "If-None-Match")
    assert(headerSet.isEmpty)
  }

  test ("Pages should be properly parsed") {
    val url = "<https://api.github.com/events?per_page=100%3Fpage%3D1&page=2>; rel=\"next\", <https://api.github.com/events?per_page=100%3Fpage%3D1&page=10>; rel=\"last\""
    val pages = service.parsePages(url)

    assert(pages.size == 2)
    assert(pages.contains(Page(2, "next")))
    assert(pages.contains(Page(10, "last")))
  }

  test ("Next and last pages should be properly retrieved") {
    val url = "<https://api.github.com/events?per_page=100%3Fpage%3D1&page=2>; rel=\"next\", <https://api.github.com/events?per_page=100%3Fpage%3D1&page=10>; rel=\"last\""
    val linkHeader = Header("Link", Array(url))
    val pages = service.parseNextAndLastPage(linkHeader)

    assert(pages._1 == 2)
    assert(pages._2 == 10)
  }

  test("Events should be properly parsed") {
    val events = service.parseEvents(sampleEvents)
    assert(events.size == 2)
  }

  test("There should be a proper duplicate check.") {
    val events = service.parseEvents(sampleEvents)
    val events2 = service.parseEvents(sampleEvents)

    val n1 = service.duplicateCheck(events)
    val n2 = service.duplicateCheck(events2)
    assert((n1 ::: n2).size == 2)
  }

  test("There should be a proper duplicate check, but only if there is space left in the queue.") {
    val service2 = new EventService(true, new StaticKeyManager(), 1)

    //ids: 7688205336 and 7688205331
    val events = service2.parseEvents(sampleEvents)

    val n1 = service2.duplicateCheck(events) //7688205331 is in the queue, both will be returned
    val n2 = service2.duplicateCheck(events) //7688205336 is in the queue, 7688205336 will be returned
    val n3 = service2.duplicateCheck(events) //7688205331 is in the queue, 7688205331 will be returned

    val nonDuplicates = (n1 ::: n2 ::: n3).map(_.id)

    assert(nonDuplicates.size == 4)
    assert(nonDuplicates.filter(_ == "7688205331").size == 2)
    assert(nonDuplicates.filter(_ == "7688205336").size == 2)
  }

  test ("Get latest events should return the latest events") {
   val eventService = spy(new EventService(false, new StaticKeyManager()))

    doReturn(GitHubResponse(sampleEvents, 200, List(Header("Link", Array(""))))).when(eventService).doPagedRequest(any(classOf[String]))

    //should stop after one run
    doReturn((1, 0)).when(eventService).parseNextAndLastPage(any(classOf[Header]))


    val events = eventService.getLatestEvents()

    assert(events.size == 2)
  }

  test ("Get latest events should return the latest events without duplicates") {
    val eventService = spy(new EventService(true, null))

    doReturn(GitHubResponse(sampleEvents, 200, List(Header("Link", Array("")))))
      .when(eventService)
      .doPagedRequest(any(classOf[String]))

    //should stop after two runs
    doReturn((0, 0))
      .when(eventService)
      .parseNextAndLastPage(any(classOf[Header]))


    val events = eventService.getLatestEvents()

    assert(events.size == 2)
  }

  test ("A paged request should be properly done.") {
    val response = service.doPagedRequest("/rate_limit") //this is a free request

    assert(response.status == 200)
  }



}
