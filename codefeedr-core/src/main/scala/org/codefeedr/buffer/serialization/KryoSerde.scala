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

package org.codefeedr.buffer.serialization

import java.lang

import com.twitter.chill.{Input, KryoBase, Output, ScalaKryoInstantiator}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** This serializer using the Kryo serialization framework for fast and small results.
  *
  * @tparam T Type of the SerDe.
  */
class KryoSerde[T <: Serializable: TypeTag: ClassTag](topic: String = "")
    extends AbstractSerde[T](topic) {

  // Lazily retrieve kryo instance.
  private lazy val kryo: KryoBase = getKryo

  /** Serializes a (generic) element using Kryo.
    *
    * @param element The element to serialized.
    * @return A serialized byte array.
    */
  override def serialize(element: T): Array[Byte] = {
    val buffer = new Array[Byte](KryoSerde.BUFFER_SIZE)
    val output = new Output(buffer)
    kryo.writeObject(output, element)

    buffer
  }

  /** Deserializes a Kryo message.
    *
    * @param message The message to deserialized.
    * @return A deserialized case class.
    */
  override def deserialize(message: Array[Byte]): T =
    kryo.readObject(new Input(message), inputClassType)

  /** Create a new Kryo instance.
    *
    * @return The instance.
    */
  private def getKryo = {
    val inst = new ScalaKryoInstantiator
    inst.setRegistrationRequired(false)
    inst.newKryo()
  }

  /** Serialize as Kafka Producer Record.
    * @return ProducerRecord.
    */
  override def serialize(element: T, timestamp: lang.Long) = {
    val buffer: Array[Byte] = new Array[Byte](KryoSerde.BUFFER_SIZE)
    val output = new Output(buffer)
    kryo.writeObject(output, element)

    new ProducerRecord(topic, serialize(element))
  }
}

/** Companion object to simply instantiation of a KryoSerde. */
object KryoSerde {
  val BUFFER_SIZE = 4096

  /** Creates new Kryo Serde. */
  def apply[T <: Serializable: ClassTag: TypeTag](
      topic: String = ""): KryoSerde[T] =
    new KryoSerde[T](topic)
}
