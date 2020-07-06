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
package org.codefeedr.plugins.mongodb.stages

import java.util
import java.util.{Calendar, Date, GregorianCalendar}

import com.github.simplyscala.{MongoEmbedDatabase, MongodProps}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.PipelineBuilder
import org.codefeedr.plugins.mongodb.MongoQuery
import org.codefeedr.stages.utilities.{SeqInput, StringInput, StringType}
import org.mongodb.scala.MongoClient
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MongoInputOutputTest
    extends FunSuite
    with MongoEmbedDatabase
    with BeforeAndAfterAll {

  val server = "mongodb://localhost:12345"
  val longString =
    """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam tincidunt posuere urna vitae lacinia. Ut fringilla erat at finibus varius. Etiam molestie nunc in dictum pulvinar. Nulla at ligula nec ante viverra varius. Aenean tincidunt enim vestibulum, ultrices diam ut, varius nulla. Sed nec massa orci. Nunc lacinia aliquam enim blandit facilisis. Quisque nec mollis ante. Etiam convallis ut nulla sed cursus. Curabitur in mauris sit amet elit sagittis sollicitudin vel nec eros. Proin condimentum, eros eget dapibus pulvinar, neque eros vestibulum dui, et sodales odio quam et nibh. Nunc egestas fringilla urna, sed pharetra magna. Cras in commodo felis.
Mauris a lacinia velit. Nam euismod, erat mollis dignissim pulvinar, orci quam lacinia magna, eget vehicula mauris nulla ac quam. Donec pellentesque quam sed bibendum gravida. Integer molestie blandit nunc sit amet rhoncus. Morbi pellentesque elit ac ante maximus, vel dapibus augue vehicula. Mauris at elementum urna. Etiam ultricies magna ut eros efficitur, in consectetur sapien sodales. Cras volutpat in felis vel lacinia. Donec eleifend est at venenatis tempus. Phasellus quis massa diam. Vivamus sit amet leo nisl. Integer sed arcu interdum, varius ex ac, luctus elit. Nullam ornare tincidunt laoreet. Vivamus lectus lectus, viverra et ex sit amet, bibendum porta diam. Etiam porttitor, justo sed vestibulum pellentesque, nisl lectus tempus nulla, id semper urna ex nec orci.
Sed dolor lectus, dapibus a semper a, pretium nec turpis. Quisque bibendum nisl non quam feugiat, quis convallis elit viverra. Aenean volutpat dolor massa, et tincidunt turpis posuere commodo. Duis elementum pulvinar nisl, sed auctor massa ornare et. Quisque rutrum dolor ac mollis porttitor. Nulla lobortis imperdiet massa, nec imperdiet quam molestie non. Quisque feugiat, ligula vel pellentesque vehicula, nulla justo vulputate mi, ut imperdiet mauris sem sed arcu. Nullam dapibus, tellus sed cursus molestie, eros nisi rhoncus nisl, vel vehicula ex nisi ut lorem. Nullam venenatis odio id enim laoreet commodo. Donec dapibus odio vel nulla efficitur viverra.Sed dolor lectus, dapibus a semper a, pretium nec turpis. Quisque bibendum nisl non quam feugiat, quis convallis elit viverra. Aenean volutpat dolor massa, et tincidunt turpis posuere commodo. Duis elementum pulvinar nisl, sed auctor massa ornare et. Quisque rutrum dolor ac mollis porttitor. Nulla lobortis imperdiet massa, nec imperdiet quam molestie non. Quisque feugiat, ligula vel pellentesque vehicula, nulla justo vulputate mi, ut imperdiet mauris sem sed arcu. Nullam dapibus, tellus sed cursus molestie, eros nisi rhoncus nisl, vel vehicula ex nisi ut lorem. Nullam venenatis odio id enim laoreet commodo. Donec dapibus odio vel nulla efficitur viverra.Sed dolor lectus, dapibus a semper a, pretium nec turpis. Quisque bibendum nisl non quam feugiat, quis convallis elit viverra. Aenean volutpat dolor massa, et tincidunt turpis posuere commodo. Duis elementum pulvinar nisl, sed auctor massa ornare et. Quisque rutrum dolor ac mollis porttitor. Nulla lobortis imperdiet massa, nec imperdiet quam molestie non. Quisque feugiat, ligula vel pellentesque vehicula, nulla justo vulputate mi, ut imperdiet mauris sem sed arcu. Nullam dapibus, tellus sed cursus molestie, eros nisi rhoncus nisl, vel vehicula ex nisi ut lorem. Nullam venenatis odio id enim laoreet commodo. Donec dapibus odio vel nulla efficitur viverra.
In hac habitasse platea dictumst. Integer nunc quam, vestibulum luctus tempor ut, condimentum in nisi. Aliquam dui erat, iaculis non lorem nec, efficitur mollis quam. Aliquam sagittis consequat magna sit amet semper. Suspendisse sed vestibulum purus. In hac habitasse platea dictumst. Pellentesque accumsan mauris id ligula tristique, at pulvinar sapien elementum.
Etiam nisl sem, egestas sit amet pretium quis, tristique ut diam. Ut dapibus sodales libero, in scelerisque nulla faucibus non. Mauris sagittis nec orci vitae hendrerit. Ut auctor non nulla in ornare. Maecenas a semper nisl. Etiam egestas, sapien nec lacinia tempor, sem nulla consequat tellus, eget dignissim magna lorem quis lacus. Suspendisse potenti. Aliquam eu justo non dolor porta faucibus. Integer id volutpat neque, vel lacinia mi. Mauris volutpat quam vitae purus pulvinar finibus. Aliquam tristique auctor semper. Pellentesque a accumsan ipsum. Nam porta elit sit amet neque cursus tempus."""

  var mongoProps: MongodProps = null

  override def beforeAll() = {
    mongoProps = mongoStart() // by default port = 12345 & version = Version.3.3.1
  } // add your own port & version parameters in mongoStart method if you need it

  override def afterAll() { mongoStop(mongoProps) }

  def clearDatabase(): Unit = {
    val client = MongoClient(server)

    val result = client.getDatabase("db").drop()
    Await.result(result.toFuture(), Duration.Inf)
  }

  test("Data should be sent to mongo successfully") {
    clearDatabase()

    val pipeline = new PipelineBuilder()
      .append(new StringInput(longString))
      .append(
        new MongoOutput[StringType]("db",
                                    "collection",
                                    server = "mongodb://localhost:12345"))
      .build()

    pipeline.startMock()
  }

  test("All data can be read from mongo") {
    StringCollectSink.reset()

    val pipeline = new PipelineBuilder()
      .append(
        new MongoInput[StringType]("db",
                                   "collection",
                                   server = "mongodb://localhost:12345"))
      .append({ x: DataStream[StringType] =>
        x.addSink(new StringCollectSink).setParallelism(1)
      })
      .build()

    pipeline.startMock()

    val list = StringCollectSink.asList
    val items = longString.split("[ \n]").toList

    assert(items.asJava.containsAll(StringCollectSink.result))
    assert(list.size == items.size)
    assert(StringCollectSink.numEventTimes == 0)
  }

  test("Throws when connection string is incorrect") {
    val pipeline = new PipelineBuilder()
      .append(new MongoInput[StringType]("db", "collection", "", "aaa"))
      .append({ x: DataStream[StringType] =>
        x.addSink(new StringCollectSink).setParallelism(1)
      })
      .build()

    assertThrows[JobExecutionException] {
      pipeline.startMock()
    }
  }

  test("Writes event time when available") {
    clearDatabase()

    val pipeline = new PipelineBuilder()
      .append(new StringInput(longString))
      .append { e: DataStream[StringType] =>
        e.assignAscendingTimestamps(_ => new Date().getTime / 1000)
      }
      .append(new MongoOutput[StringType]("db",
                                          "collection",
                                          server = "mongodb://localhost:12345"))
      .build()

    pipeline.startMock()
  }

  test("Reads event time") {
    StringCollectSink.reset()

    val pipeline = new PipelineBuilder()
      .append(
        new MongoInput[StringType]("db",
                                   "collection",
                                   server = "mongodb://localhost:12345"))
      .append({ x: DataStream[StringType] =>
        x.addSink(new StringCollectSink).setParallelism(1)
      })
      .build()

    pipeline.startMock()

    assert(StringCollectSink.result.size == StringCollectSink.numEventTimes)
  }

  test("Can filter on event time (1)") {
    clearDatabase()

    val list = Seq[TestEvent](
      TestEvent("klaas", new GregorianCalendar(1998, Calendar.JUNE, 1).getTime),
      TestEvent("nagellak",
                new GregorianCalendar(1998, Calendar.SEPTEMBER, 1).getTime),
      TestEvent("verdieping",
                new GregorianCalendar(1997, Calendar.SEPTEMBER, 12).getTime)
    )

    val pipeline = new PipelineBuilder()
      .append(new SeqInput[TestEvent](list))
      .append { e: DataStream[TestEvent] =>
        e.assignAscendingTimestamps(x => x.time.getTime / 1000)
      }
      .append(new MongoOutput[TestEvent]("db",
                                         "collection",
                                         server = "mongodb://localhost:12345"))
      .build()

    pipeline.startMock()
  }

  test("Can filter on event time (2)") {
    TestEventCollectSink.reset()

    val query =
      MongoQuery.from(new GregorianCalendar(1998, Calendar.JANUARY, 1).getTime)

    val pipeline = new PipelineBuilder()
      .append(
        new MongoInput[TestEvent]("db",
                                  "collection",
                                  "",
                                  "mongodb://localhost:12345",
                                  query))
      .append({ x: DataStream[TestEvent] =>
        x.addSink(new TestEventCollectSink).setParallelism(1)
      })
      .build()

    pipeline.startMock()

    assert(TestEventCollectSink.result.size == 2)
    assert(TestEventCollectSink.result.containsAll(Seq("klaas", "nagellak").asJava))
  }

}

case class TestEvent(name: String, time: Date)

object StringCollectSink {
  var result = new util.ArrayList[String]() //mutable list
  var numEventTimes = 0

  def reset(): Unit = {
    result = new util.ArrayList[String]()
    numEventTimes = 0
  }

  def asList: List[String] = result.asScala.toList
}

class StringCollectSink extends SinkFunction[StringType] {

  override def invoke(value: StringType, context: Context[_]): Unit = {
    synchronized {
      StringCollectSink.result.add(value.value)

      if (context.timestamp() != null) {
        StringCollectSink.numEventTimes += 1
      }
    }
  }
}

object TestEventCollectSink {
  var result = new util.ArrayList[String]() //mutable list
  var numEventTimes = 0

  def reset(): Unit = {
    result = new util.ArrayList[String]()
    numEventTimes = 0
  }

  def asList: List[String] = result.asScala.toList
}

class TestEventCollectSink extends SinkFunction[TestEvent] {

  override def invoke(value: TestEvent, context: Context[_]): Unit = {
    synchronized {
      TestEventCollectSink.result.add(value.name)

      if (context.timestamp() != null) {
        TestEventCollectSink.numEventTimes += 1
      }
    }
  }
}
