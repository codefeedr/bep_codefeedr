package org.codefeedr.buffer.serialization

import org.codefeedr.pipeline.PipelineItem
import org.scalatest.FunSuite
import org.apache.flink.api.scala._

class SerializerTest extends FunSuite {

  case class Item() extends PipelineItem

  test("Should recognise JSON") {
    val serde = Serializer.getSerde[Item](Serializer.JSON)

    assert(serde.isInstanceOf[JSONSerde[Item]])
  }

  test("Should recognise Bson") {
    val serde = Serializer.getSerde[Item](Serializer.BSON)

    assert(serde.isInstanceOf[BsonSerde[Item]])
  }

  test("Should recognise Kryo") {
    val serde = Serializer.getSerde[Item](Serializer.KRYO)

    assert(serde.isInstanceOf[KryoSerde[Item]])
  }

  test("Should default to JSON") {
    val serde = Serializer.getSerde[Item]("YAML")

    assert(serde.isInstanceOf[JSONSerde[Item]])
  }
}
