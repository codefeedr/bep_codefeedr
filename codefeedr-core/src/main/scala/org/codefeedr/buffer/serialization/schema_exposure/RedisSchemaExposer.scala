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
package org.codefeedr.buffer.serialization.schema_exposure

import java.net.URI

import com.redis.RedisClient
import org.apache.avro.Schema

/** Exposes (Avro) schema's to Redis.
  *
  * @param host the server host of Redis.
  * @param root the root location of the schema's
  *             DEFAULT: /codefeedr:schemas
  */
class RedisSchemaExposer(host: String, root: String = "codefeedr:schemas")
    extends SchemaExposer {

  private var connection: RedisClient = _

  //connect to the redis client
  connect()

  /** Connect with the RedisClient. */
  private def connect(): Unit = {
    val uri = new URI(host)
    connection = new RedisClient(uri)
  }

  /** Stores a schema bound to a subject.
    *
    * @param schema The schema belonging to that topic.
    * @param subject The subject belonging to that schema.
    * @return True if correctly saved.
    */
  override def put(schema: Schema, subject: String): Boolean = {
    connection.set(s"$root:$subject", schema.toString(true))
  }

  /** Get a schema based on a subject.
    *
    * @param subject The subject the schema belongs to.
    * @return None if no schema is found or an invalid schema. Otherwise it returns the schema.
    */
  override def get(subject: String): Option[Schema] = {
    val schemaString = connection.get[String](s"$root:$subject")

    //if no string is found, return None
    if (schemaString.isEmpty) return None

    //parse the string into a Schema
    parse(schemaString.get)
  }

  /** Deletes a schema.
    *
    * @param subject The subject the schema belongs to.
    * @return True if successfully deleted, otherwise false.
    */
  override def delete(subject: String): Boolean = {
    val deleted = connection.del(s"$root:$subject")

    //if deleted doesnt return anything or less than 1 key is deleted, return false
    if (deleted.isEmpty || deleted.get < 1) {
      return false
    }

    true
  }

  /** Deletes all schemas 'below' the root. */
  override def deleteAll(): Unit = {
    val keys = connection.keys(s"[$root]*")

    keys.get
      .map(_.get)
      .foreach(connection.del(_))
  }
}
