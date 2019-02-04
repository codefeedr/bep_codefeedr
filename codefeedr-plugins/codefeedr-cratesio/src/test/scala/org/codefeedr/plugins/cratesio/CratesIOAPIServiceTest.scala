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

package org.codefeedr.plugins.cratesio

import java.io.InputStream

import org.mockito.Matchers.any
import org.mockito.Mockito.{doReturn, spy}

import scala.io.Source
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers._
import scalaj.http.HttpResponse

import scala.util.Try

class CratesIOAPIServiceTest extends FunSuite with BeforeAndAfter {

  var libcString : String = _
  var service : CratesIOAPIService = _

  before {
    val stream : InputStream = getClass.getResourceAsStream("/libc.json")
    libcString = Source.fromInputStream(stream).getLines.mkString
    service = new CratesIOAPIService
  }

  test ("Parse Cargo.io JSON replies") {

    val result = service.parseResponse(libcString).get

    assert(result.crate.versions.length == 55)
    assert(result.versions.length == 55)
    assert(result.crate.badges.length == 2)
    assert(result.crate.links.owner_user == "/api/v1/crates/libc/owner_user")
  }

  test ("Parse crate index commit messages") {
    Map("Updating crate `btc-transaction-utils#0.2.0`" -> Try("btc-transaction-utils"),
        "Updating crate `gcollections#1.1.0`" -> Try("gcollections"),
        "Updating crate `sbr#0.1.1`" -> Try("sbr"),
        "Updating crate `error-chain-mini-derive#0.2.0`" -> Try("error-chain-mini-derive"),
        "Updating crate `error-chain-mini-derive#0.2.0" -> Try("error-chain-mini-derive")
    ).foreach(x => assert(service.extractCrateFromCommitMsg(x._1) == x._2))

    service.extractCrateFromCommitMsg("#0.2.0") shouldBe 'failure
    service.extractCrateFromCommitMsg("test#0.2.0") shouldBe 'failure
  }

  test ("Request to the crates.io API") {
    val apiService = spy(new CratesIOAPIService())

    doReturn(new HttpResponse(libcString, 200, Map()))
      .when(apiService)
      .crateAPIRequest(any(classOf[String]))

    val libc = apiService.crateAPIRequest("libc")

    assert(apiService.parseResponse(libc.body).get.crate.name == "libc")
  }
}
