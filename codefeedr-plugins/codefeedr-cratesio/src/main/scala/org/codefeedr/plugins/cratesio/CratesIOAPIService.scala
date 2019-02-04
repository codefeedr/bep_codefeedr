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

import org.codefeedr.plugins.cratesio.CargoProtocol.CrateInfo
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scalaj.http.{Http, HttpOptions, HttpResponse}
import scala.util.{Failure, Success, Try}

/**
  * A set of methods to query the crates.io API for a single package information
  */
class CratesIOAPIService extends Serializable {

  val BASE_URL = "https://crates.io/api/v1/crates/"

  /**
    * Retrieve CrateInfo from crates API.
    *
    * @param crateName the name of the crate.
    * @return its CrateInfo.
    */
  def crateInfo(crateName: String) : Option[CrateInfo] =
    parseResponse(crateAPIRequest(crateName).body)

  /**
    * Requests crate from the crates API.
    *
    * @param crate the crate name.
    * @return a http response.
    */
  def crateAPIRequest(crate: String): HttpResponse[String] =
    Http(BASE_URL + crate).options(HttpOptions.allowUnsafeSSL).
        asString

  /**
    * Parses JSON response.
    *
    * @param crateInfoJSON the JSON string.
    * @return parsed CrateInfo.
    */
  def parseResponse(crateInfoJSON: String): Option[CrateInfo] = {
    //necessary for JSON parsing
    implicit val defaultFormats = DefaultFormats

    if (crateInfoJSON == "") return None

    Some(parse(crateInfoJSON).extract[CrateInfo])
  }


  /**
    * Extract the crate from a commit message.
    *
    * @param msg the commit message.
    * @return a potential crate.
    */
  def extractCrateFromCommitMsg(msg: String) : Try[String] =
    try {
      println(msg)
      Success(msg.split("`")(1).split("#")(0))
    } catch {
      case e: Exception => Failure(e)
    }
}

