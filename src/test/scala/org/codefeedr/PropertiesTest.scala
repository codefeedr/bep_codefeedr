package org.codefeedr

import org.scalatest.FunSuite
import Properties._

class PropertiesTest extends FunSuite {

  test("An empty properties should have no keys") {
    val props = new Properties()

    assert(props.keys().isEmpty)
  }

  test("An added value should show up in the keys list") {
    val props = new Properties()
      .set("key", "value")

    assert(props.keys().contains("key"))
  }

  test("Setting multiple keys is supported") {
    val props = new Properties()
      .set("keyOne", "one")
      .set("two", "another")

    assert(props.get("keyOne").get == "one")
    assert(props.get("two").get == "another")
  }

  test("An added value should be retrievable again") {
    val props = new Properties()
      .set("key", "value")

    assert(props.get("key").get == "value")
  }

  test("When no default is given, a non-existing key should return null") {
    val props = new Properties()

    assert(props.get("key").isEmpty)
  }

  test("A Java properties list created from the properties should be comparable in content") {
    val props = new Properties()
      .set("key", "value")

    val jProps = props.toJavaProperties

    val propsSet = props.keys().toSet
    val jPropsSet = jProps.keySet().toArray.toSet

    assert(propsSet == jPropsSet)
  }

  test("Comparing to non-Properties gives non-equality") {
    val props = new Properties()

    //noinspection ComparingUnrelatedTypes
    assert(props != 5)
  }

  test("Comparing two non-equal properties should result in inequality") {
    val props = new Properties()
    val props2 = new Properties()
      .set("a", "b")

    assert(props != props2)
  }

  test("Comparing two equal properties should result in equality") {
    val props = new Properties()
      .set("a", "b")
    val props2 = new Properties()
      .set("a", "b")

    assert(props == props2)
  }

  test("Types should be converted automatically to and from") {
    val props = new Properties()
      .set[Boolean]("bool", true)

    assert(props.get("bool").get == "true")
    assert(props.get[Boolean]("bool").get)
  }

  test("Getting with defaults") {
    val props = new Properties()
      .set[String]("foo", "hello")

    assert(props.getOrElse[String]("foo", "bye") == "hello")
    assert(props.getOrElse[String]("bar", "bye") == "bye")
  }

  test("Outputs readable string") {
    val props = new Properties()

    assert(props.toString == "Map()")
  }

  test("Should give correct value when checking content") {
    var props = new Properties()

    assert(!props.has("a"))

    props = props.set("foo", "bar")
    assert(props.has("foo"))
    assert(!props.has("bar"))
  }
}
