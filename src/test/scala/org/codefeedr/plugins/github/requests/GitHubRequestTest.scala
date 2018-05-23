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

import org.codefeedr.plugins.github.GitHubEndpoints
import org.codefeedr.utilities.Http
import org.mockito.Mockito.spy
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.spy
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}

import scalaj.http.{HttpRequest, HttpResponse}

class GitHubRequestTest extends FunSuite with BeforeAndAfter with MockitoSugar {

  var defaultRequest : GitHubRequest = _

  before {
    defaultRequest = new GitHubRequest("/test", List(Header("test", Array("test")), Header("test2", Array("test", "test2"))))
  }

  test("200 status should be properly forwarded") {
    val simpleResponse = new GitHubResponse("", 200, List())
    val result = defaultRequest.handleErrorCodes(simpleResponse)

    assert(result == simpleResponse)
  }

  test("304 status should be properly forwarded") {
    val simpleResponse = new GitHubResponse("", 304, List())
    val result = defaultRequest.handleErrorCodes(simpleResponse)

    assert(result == simpleResponse)
  }

  test("Unknown status should throw an exception") {
    val simpleResponse = new GitHubResponse("", 666, List())

    assertThrows[GitHubRequestException] {
      defaultRequest.handleErrorCodes(simpleResponse)
    }
  }

  test("A HTTP response should be properly parsed") {
    val http = HttpResponse[String]("body", 200, Map("testHeader" -> IndexedSeq("test")))
    val response = defaultRequest.parseResponse(http)

    assert(response.body == "body")
    assert(response.status == 200)
    assert(response.headers.size == 1)
    assert(response.headers(0).key == "testHeader")
    assert(response.headers(0).value.size == 1)
    assert(response.headers(0).value.contains("test"))
  }

  test("A HTTP Request should be properly build") {
    val request = defaultRequest.buildRequest()

    assert(request.headers.contains(("test", "test")))
    assert(request.url == GitHubEndpoints.DEFAULT_URL + "/test")
    assert(request.headers.contains(("Accept", "application/vnd.github.v3+json")))
  }

  test("A request should be properly handled") {
    val mockHttp = mock[HttpRequest]
    val request = spy(defaultRequest)
    val simpleResponse = HttpResponse[String]("body", 200, Map("testHeader" -> IndexedSeq("test")))

    when(request.buildRequest()).thenReturn(mockHttp)
    when(mockHttp.asString).thenReturn(simpleResponse)

    val response = request.request()

    assert(response.body == "body")
    assert(response.status == 200)
    assert(response.headers.size == 1)
    assert(response.headers(0).key == "testHeader")
    assert(response.headers(0).value.size == 1)
    assert(response.headers(0).value.contains("test"))
  }

}
