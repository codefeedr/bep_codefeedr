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
package org.codefeedr.util.schema_exposure

import com.github.sebruck.EmbeddedRedis
import org.scalatest.BeforeAndAfterAll
import redis.embedded.RedisServer

class RedisSchemaExposerTest
  extends SchemaExposerTest
    with BeforeAndAfterAll
    with EmbeddedRedis {
  var redis: RedisServer = _
  var redisPort: Int = 0

  // Before all tests, setup an embedded redis
  override def beforeAll(): Unit = {
    redis = startRedis()
    redisPort = redis.ports().get(0)
  }

  // After all tests, stop embedded redis
  override def afterAll(): Unit = {
    stopRedis(redis)
  }

  override def getSchemaExposer: SchemaExposer =
    new RedisSchemaExposer(s"redis://localhost:$redisPort")
}
