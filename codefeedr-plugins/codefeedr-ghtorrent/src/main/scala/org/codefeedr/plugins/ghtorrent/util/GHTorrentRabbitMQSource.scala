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
package org.codefeedr.plugins.ghtorrent.util

import java.io.IOException
import java.util

import com.rabbitmq.client._
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{
  MultipleIdsMessageAcknowledgingSourceBase,
  SourceFunction
}
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig
import org.apache.flink.util.Preconditions
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source
import collection.JavaConverters._

/** This class is based on the orginal Flink RabbitMQSource.
  * It creates a queue bound to a set of routing keys according to the GHTorrent protocol.
  *
  * @param username your username to specify to GHTorrent.
  * @param host the host name, default is localhost.
  * @param port the port, default is 5672.
  * @param routingKeysFile the location of the routingKeysFile (in the resources directory).
  * @param virtualHost the virtual host of GHTorrent.
  * @param password your password to specify to GHTorrent.
  */
class GHTorrentRabbitMQSource(username: String = "streamer",
                              host: String = "localhost",
                              port: Int = 5672,
                              routingKeysFile: String = "routing_keys.txt",
                              virtualHost: String = "/",
                              password: String = "streamer")
    extends MultipleIdsMessageAcknowledgingSourceBase[String, String, Long](
      classOf[String])
    with ResultTypeQueryable[String] {

  // Logger instance.
  private val LOG: Logger =
    LoggerFactory.getLogger(classOf[GHTorrentRabbitMQSource])

  // We parse it into a String in the format: routing_key#body
  private val schema: SimpleStringSchema = new SimpleStringSchema()

  // Configuration of RabbitMQ (also according to GHTorrent spec).
  val rmConnectionConfig: RMQConnectionConfig =
    new RMQConnectionConfig.Builder()
      .setHost(host)
      .setPort(port)
      .setVirtualHost(virtualHost)
      .setUserName(username)
      .setPassword(password)
      .build()

  @transient
  var connection: Connection = null

  @transient
  var channel: Channel = null

  @transient
  var autoAck: Boolean = false

  @transient @volatile
  private var running: Boolean = false

  /** Setting queueName according to GHTorrent specification **/
  private val queueName = username + "_queue"

  /** Parse all routing keys from the file. We assume they are separated by new lines. **/
  val routingKeys = parseRoutingKeys()

  /** Name of the exchange, this is a requirement by the GHTorrent streaming service. **/
  val exchangeName = "ght-streams"

  /**
    * Setups queue according to http://ghtorrent.org/streaming.html
    */
  def setupQueue(): Unit = {
    // First of all, we declare an exchange with the correct name and type.
    channel.exchangeDeclare(exchangeName, "topic", true)

    // Create a queue with auto delete.
    channel.queueDeclare(username + "_queue",
                         false,
                         false,
                         true,
                         new util.HashMap[String, AnyRef]())

    // For each routing key, bind it to the channel.
    routingKeys.foreach(channel.queueBind(queueName, exchangeName, _))
  }

  /** Opens this source by creating a new connection and channel.
    *
    * @param parameters the parameters of this Flink job.
    */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val factory: ConnectionFactory = getFactory()

    try {

      // Create connection and channel.
      connection = factory.newConnection()
      channel = connection.createChannel()

      if (channel == null) {
        throw new RuntimeException("None of RabbitMQ channels are available.")
      }

      // Setups a queue.
      setupQueue()

      // Find out if checkpointing is enabled, then enable transactional mode.
      val runtimeContext: RuntimeContext = this.getRuntimeContext()
      if (runtimeContext.isInstanceOf[StreamingRuntimeContext] && runtimeContext
            .asInstanceOf[StreamingRuntimeContext]
            .isCheckpointingEnabled) {
        autoAck = false
        channel.txSelect() // enable transaction mode
      } else {
        autoAck = true
      }

    } catch {
      case e: IOException =>
        throw new RuntimeException(
          "Cannot create a RabbitMQ connection at " + rmConnectionConfig.getHost,
          e)
    }

    running = true
  }

  /** Closes the RabbitMQ channel */
  override def close(): Unit = {
    super.close()

    try {
      if (connection != null) {
        connection.close()
      }
    } catch {
      case e: IOException =>
        throw new RuntimeException(
          "Error while closing RabbitMQ connection at " + rmConnectionConfig.getHost,
          e)
    }

  }

  /** Runs the GHTorrentSource by listening to the setup queue.
    *
    * @param ctx the Flink context.
    */
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    LOG.debug("Starting RabbitMQ source with autoAck status: " + autoAck)

    val consumerTag = "codefeedrConsumerTag" //we keep this tag to also cancel the consumption.

    channel.basicConsume(
      queueName,
      autoAck,
      "codefeedrConsumerTag",
      new DefaultConsumer(channel) { // Setup a consumer callback.
        override def handleDelivery(consumerTag: String,
                                    envelope: Envelope,
                                    properties: AMQP.BasicProperties,
                                    body: Array[Byte]): Unit = {

          ctx.getCheckpointLock.synchronized {

            // Get the routing key and the body of the consumed message.
            val routingKey = envelope.getRoutingKey()
            val result = schema.deserialize(body)

            if (!autoAck) { //If autoAck is disabled, we provide the delivery tag to a list of sessionIds.
              val deliveryTag = envelope.getDeliveryTag

              /**  if (usesCorrelationId) {
                val correlationId = properties.getCorrelationId

                Preconditions.checkNotNull(
                  correlationId,
                  "RabbitMQ source was instantiated " + "with usesCorrelationId set to true but a message was received with " + "correlation id set to null!")
                if (!alreadyProcessed(correlationId)) { //Ignore if we already processed this message.
                  return
                }
              } **/
              getSessionIds
                .add(deliveryTag) //Add the delivery tag so that it can be checkpointed.
            }

            // Collect element in the form routing_key#body
            ctx.collect(s"$routingKey#$result")
          }

        }

      }
    )

    while (running) {} // Make sure we keep running.
    channel.basicCancel(consumerTag) // After running, cancel the channel.
  }

  /** Called by the checkpoint to acknowledge the seen id's. */
  override def acknowledgeSessionIDs(sessionIds: util.List[Long]): Unit = {
    try {
      sessionIds.asScala.foreach(channel.basicAck(_, false))
      channel.txCommit()
    } catch {
      case e: IOException =>
        throw new RuntimeException(
          "Messages could not be acknowledged during checkpoint creation.",
          e)
    }
  }

  /** Get the produced type. */
  override def getProducedType: TypeInformation[String] = schema.getProducedType

  /** Cancel the source. */
  override def cancel(): Unit = running = false

  /** Creates factory based on RMConnectionConfig. */
  def getFactory() = rmConnectionConfig.getConnectionFactory()

  /** Return a list of session ids */
  def getSessionIds = sessionIds

  /** Check if a message has already been processed. */
  def alreadyProcessed(correlationId: String) = addId(correlationId)

  /** Parses all routing keys from the file.
    *
    * @return a list of routing keys.
    */
  private def parseRoutingKeys(): List[String] =
    Source.fromResource(routingKeysFile).mkString.split("\n").toList
}
