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

package org.codefeedr.plugins.elasticsearch

import java.net.{InetAddress, InetSocketAddress, URI}
import java.nio.charset.StandardCharsets

import com.sksamuel.avro4s.FromRecord
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.codefeedr.pipeline.{OutputStage, PipelineItem, StageAttributes}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.json4s.NoTypeHints
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.Serialization

import scala.reflect.{ClassTag, Manifest}

/**
  * An output stage that pushes all elements as JSON to an Elastic Search cluster
  *
  * @param index Elastic Search index to push the data to
  * @param servers Optional set of server addresses (defaults to es://localhost:9300)
  * @param config Extra configuration options for the Elastic Search client
  * @param attributes Optional stage attributes
  * @tparam T Input type
  */
class ElasticSearchOutputStage[T <: PipelineItem : ClassTag : Manifest : FromRecord](index: String,
                                                                                     servers: Set[String] = Set(),
                                                                                     config: Map[String, String] = Map(),
                                                                                     attributes: StageAttributes = StageAttributes())
  extends OutputStage[T](attributes) {

  override def main(source: DataStream[T]): Unit = {
    val config = createConfig()
    val transportAddresses = createTransportAddresses()

    source.addSink(new ElasticsearchSink(config, transportAddresses, new ElasticSearchSink[T](index)))
  }

  def createConfig(): java.util.HashMap[String, String] = {
    val config = new java.util.HashMap[String, String]

    // This instructs the sink to emit after every element, otherwise they would be buffered
    config.put("bulk.flush.max.actions", "1")

    config
  }

  /**
    * Create elastic search host address list
    *
    * @return List
    */
  def createTransportAddresses(): java.util.ArrayList[InetSocketAddress] = {
    val transportAddresses = new java.util.ArrayList[InetSocketAddress]

    if (servers.isEmpty) {
      transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))
    }

    for (server <- servers) {
      val uri = new URI(server)

      if (uri.getScheme == "es") {
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName(uri.getHost), uri.getPort))
      }
    }

    transportAddresses
  }

}

/**
  * An elastic search sink
  *
  * @param index Index to be used in ElasticSearch
  * @tparam T Type of input
  */
private class ElasticSearchSink[T <: PipelineItem : ClassTag : Manifest : FromRecord](index: String) extends ElasticsearchSinkFunction[T] {

  implicit lazy val formats = Serialization.formats(NoTypeHints) ++ JavaTimeSerializers.all

  def createIndexRequest(element: T): IndexRequest = {
    val bytes = serialize(element)

    Requests.indexRequest()
      .index(index)
      .`type`("json")
      .source(bytes)
  }

  override def process(element: T, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
    indexer.add(createIndexRequest(element))
  }

  /**
    * Serialize an element into JSON
    *
    * @param element Element
    * @return JSON bytes
    */
  def serialize(element: T): Array[Byte] = {
    val bytes = Serialization.write[T](element)(formats)
    bytes.getBytes(StandardCharsets.UTF_8)
  }

}
