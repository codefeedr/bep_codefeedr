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
package org.codefeedr.plugins.log

import java.time.LocalDateTime

import org.codefeedr.pipeline.PipelineItem

/**
  * Pipeline item that contains an entrie from an Apache Request log.
  * @param ip IP Address of the requester
  * @param date Date that the request is made
  * @param method HTTP method
  * @param path Path of request
  * @param version Http version
  * @param status HTTP Response status
  * @param amountOfBytes Size of the request in bytes
  * @param referer Referer
  * @param userAgent Information about the user agent that made the request
  */
case class ApacheAccessLogItem(ip: String,
                               date: LocalDateTime,
                               method: String,
                               path: String,
                               version: String,
                               status: Int,
                               amountOfBytes: Int,
                               referer: String,
                               userAgent: String) extends PipelineItem