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
package org.codefeedr

import scala.language.implicitConversions
import scala.collection.JavaConverters._

/** Implicit conversions for Properties. */
object Properties {
  // Conversion implicits
  implicit def stringToBoolean(str: String): Boolean = str.toBoolean
  implicit def booleanToString(bool: Boolean): String = bool.toString
}

/** Object containing configuration properties. */
class Properties(private val contents: Map[String, String] = Map()) {

  /**
    * Get a value converted to an implicitly converted type.
    *
    * @param key Key
    * @param convert Conversion implicit
    * @tparam T Type of the value
    * @return Value option
    */
  def get[T](key: String)(implicit convert: String => T): Option[T] = {
    val option = contents.get(key)
    if (option.isEmpty) {
      return None
    }

    Option(option.get)
  }

  /**
    * Get a value converted to an implicity cobverted type with a default value.
    *
    * @param key Key
    * @param default Default value
    * @param convert Conversion implicit
    * @tparam T Type of the value
    * @return Value
    */
  def getOrElse[T](key: String, default: T)(
      implicit convert: String => T): T = {
    val option = contents.get(key)
    if (option.isEmpty) {
      return default
    }

    option.get
  }

  /**
    * Get whether a key has any value
    *
    * @param key Key
    * @return True when it exists
    */
  def has(key: String): Boolean = contents.isDefinedAt(key)

  /**
    * Set a value of an implicitly converted type
    *
    * @param key Key
    * @param value Value
    * @param convert Conversion implicit
    * @tparam T Type of value
    * @return New immutable properties
    */
  def set[T](key: String, value: T)(implicit convert: T => String): Properties =
    new Properties(contents + (key -> value))

  /** Merges two Properties object.
    *
    * @param properties the second properties object. These properties will override the keys of *this*.
    * @return new properties.
    */
  def merge(properties: Properties): Properties = {
    val keys = properties.keys()
    var newProps = this

    for (key <- keys) {
      newProps = newProps.set(key, properties.get(key).get)
    }

    newProps
  }

  /**
    * Get a set if keys in this properties.
    *
    * @return
    */
  def keys(): Set[String] =
    contents.keys.toSet

  override def equals(that: Any): Boolean = that match {
    case that: Properties => that.contents == contents
    case _                => false
  }

  /**
    * Acquire a Java Properties object containing the same properties as this object.
    *
    * @note Some Flink internal systems use Properties. Only use this for those.
    *
    * @return a Java Properties object
    */
  def toJavaProperties: java.util.Properties = {
    val props = new java.util.Properties()

    contents.foreach {
      case (key, value) =>
        props.setProperty(key, value)
    }

    props
  }

  /** Get contents of the this properties set. */
  def getContents: Map[String, String] = contents

  override def toString: String = contents.toString()
}
