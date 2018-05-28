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
 */

package org.codefeedr.plugins.mongodb.keymanager

import java.net.URI
import java.util.Date

import org.codefeedr.keymanager.{KeyManager, ManagedKey}
import org.mongodb.scala.{MongoClient, MongoCollection, SingleObservable}
import org.mongodb.scala.model.Filters._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.UpdateOptions

/**
  * Key manager implementation using mongoDB as a backend.
  *
  * @param server URI to the server
  */
class MongoKeyManager(database: String = "db",
                      collection: String = "codefeedrKeyManager",
                      server: URI = null)
  extends KeyManager {

  // Serialization handling for Mongo BSON
  private val codecRegistry = fromRegistries(fromProviders(classOf[MongoManagedKey]), DEFAULT_CODEC_REGISTRY )

  override def request(target: String, numberOfCalls: Int): Option[ManagedKey] = {
    require(target != null, "target cannot be null")

    refreshKeys(target)

    // Find the best fitting key
    val search = and(equal("target", target), gte("numCallsLeft", numberOfCalls))
    val update = Document("$inc" -> Document("numCallsLeft" -> -numberOfCalls))
    val action = getCollection
        .findOneAndUpdate(search, update)

    val result = await(action)

    // None found
    if (result == null) {
      return None
    }

    Some(ManagedKey(result.key, result.numCallsLeft - numberOfCalls))
  }

  def refreshKeys(target: String): Unit = {
    // TODO
  }

  private def await[T](value: SingleObservable[T]) =
    Await.result(value.toFuture(), Duration.Inf)

  /**
    * Add a key to the key manager
    *
    * @param target Target
    * @param key The key
    * @param limit Number of calls per interval
    * @param interval Interval of key refreshes
    * @param refreshTime Next refresh time
    * @return
    */
  def add(target: String, key: String, limit: Int, interval: Int, refreshTime: Date): Unit = {
    val col = getCollection

    val managedKey = MongoManagedKey(target, key, limit, limit, interval, refreshTime)

    await(getCollection.insertOne(managedKey))
  }

  /**
    * Clear all keys from a target.
    * @param target
    */
  def clear(target: String): Unit =
    await(getCollection.deleteMany(equal("target", target)))

  /**
    * Clear the whole key manager.
    */
  def clear(): Unit =
    await(getCollection.drop())

  /**
    * Get the mongo collection.
    *
    * It might not yet exist.
    *
    * @return Mongo Collection
    */
  private def getCollection: MongoCollection[MongoManagedKey] = {
    val client = if (server == null) MongoClient() else MongoClient(server.toString)

    val databaseObject = client.getDatabase(database).withCodecRegistry(codecRegistry)

    databaseObject.getCollection(collection)
  }
}