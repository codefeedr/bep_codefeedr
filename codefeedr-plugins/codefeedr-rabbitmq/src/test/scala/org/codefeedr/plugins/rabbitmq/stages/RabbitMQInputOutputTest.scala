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
package org.codefeedr.plugins.rabbitmq.stages

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.codefeedr.pipeline
import org.codefeedr.plugins.rabbitmq.RMQSinkDurable
import org.codefeedr.stages.utilities.StringType
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import org.mockito.Matchers._
import org.mockito.Mockito._

import scala.collection.JavaConverters._

class RabbitMQInputOutputTest extends FunSuite with MockitoSugar {

  val longString =
    """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam tincidunt posuere urna vitae lacinia. Ut fringilla erat at finibus varius. Etiam molestie nunc in dictum pulvinar. Nulla at ligula nec ante viverra varius. Aenean tincidunt enim vestibulum, ultrices diam ut, varius nulla. Sed nec massa orci. Nunc lacinia aliquam enim blandit facilisis. Quisque nec mollis ante. Etiam convallis ut nulla sed cursus. Curabitur in mauris sit amet elit sagittis sollicitudin vel nec eros. Proin condimentum, eros eget dapibus pulvinar, neque eros vestibulum dui, et sodales odio quam et nibh. Nunc egestas fringilla urna, sed pharetra magna. Cras in commodo felis.
Mauris a lacinia velit. Nam euismod, erat mollis dignissim pulvinar, orci quam lacinia magna, eget vehicula mauris nulla ac quam. Donec pellentesque quam sed bibendum gravida. Integer molestie blandit nunc sit amet rhoncus. Morbi pellentesque elit ac ante maximus, vel dapibus augue vehicula. Mauris at elementum urna. Etiam ultricies magna ut eros efficitur, in consectetur sapien sodales. Cras volutpat in felis vel lacinia. Donec eleifend est at venenatis tempus. Phasellus quis massa diam. Vivamus sit amet leo nisl. Integer sed arcu interdum, varius ex ac, luctus elit. Nullam ornare tincidunt laoreet. Vivamus lectus lectus, viverra et ex sit amet, bibendum porta diam. Etiam porttitor, justo sed vestibulum pellentesque, nisl lectus tempus nulla, id semper urna ex nec orci.
Sed dolor lectus, dapibus a semper a, pretium nec turpis. Quisque bibendum nisl non quam feugiat, quis convallis elit viverra. Aenean volutpat dolor massa, et tincidunt turpis posuere commodo. Duis elementum pulvinar nisl, sed auctor massa ornare et. Quisque rutrum dolor ac mollis porttitor. Nulla lobortis imperdiet massa, nec imperdiet quam molestie non. Quisque feugiat, ligula vel pellentesque vehicula, nulla justo vulputate mi, ut imperdiet mauris sem sed arcu. Nullam dapibus, tellus sed cursus molestie, eros nisi rhoncus nisl, vel vehicula ex nisi ut lorem. Nullam venenatis odio id enim laoreet commodo. Donec dapibus odio vel nulla efficitur viverra.Sed dolor lectus, dapibus a semper a, pretium nec turpis. Quisque bibendum nisl non quam feugiat, quis convallis elit viverra. Aenean volutpat dolor massa, et tincidunt turpis posuere commodo. Duis elementum pulvinar nisl, sed auctor massa ornare et. Quisque rutrum dolor ac mollis porttitor. Nulla lobortis imperdiet massa, nec imperdiet quam molestie non. Quisque feugiat, ligula vel pellentesque vehicula, nulla justo vulputate mi, ut imperdiet mauris sem sed arcu. Nullam dapibus, tellus sed cursus molestie, eros nisi rhoncus nisl, vel vehicula ex nisi ut lorem. Nullam venenatis odio id enim laoreet commodo. Donec dapibus odio vel nulla efficitur viverra.Sed dolor lectus, dapibus a semper a, pretium nec turpis. Quisque bibendum nisl non quam feugiat, quis convallis elit viverra. Aenean volutpat dolor massa, et tincidunt turpis posuere commodo. Duis elementum pulvinar nisl, sed auctor massa ornare et. Quisque rutrum dolor ac mollis porttitor. Nulla lobortis imperdiet massa, nec imperdiet quam molestie non. Quisque feugiat, ligula vel pellentesque vehicula, nulla justo vulputate mi, ut imperdiet mauris sem sed arcu. Nullam dapibus, tellus sed cursus molestie, eros nisi rhoncus nisl, vel vehicula ex nisi ut lorem. Nullam venenatis odio id enim laoreet commodo. Donec dapibus odio vel nulla efficitur viverra.
In hac habitasse platea dictumst. Integer nunc quam, vestibulum luctus tempor ut, condimentum in nisi. Aliquam dui erat, iaculis non lorem nec, efficitur mollis quam. Aliquam sagittis consequat magna sit amet semper. Suspendisse sed vestibulum purus. In hac habitasse platea dictumst. Pellentesque accumsan mauris id ligula tristique, at pulvinar sapien elementum.
Etiam nisl sem, egestas sit amet pretium quis, tristique ut diam. Ut dapibus sodales libero, in scelerisque nulla faucibus non. Mauris sagittis nec orci vitae hendrerit. Ut auctor non nulla in ornare. Maecenas a semper nisl. Etiam egestas, sapien nec lacinia tempor, sem nulla consequat tellus, eget dignissim magna lorem quis lacus. Suspendisse potenti. Aliquam eu justo non dolor porta faucibus. Integer id volutpat neque, vel lacinia mi. Mauris volutpat quam vitae purus pulvinar finibus. Aliquam tristique auctor semper. Pellentesque a accumsan ipsum. Nam porta elit sit amet neque cursus tempus."""

  /**
  test("Data should be sent to RabbitMQ successfully") {
//    clearDatabase()

    val pipeline = new PipelineBuilder()
      .append(new StringInput(longString))
      .append(new RabbitMQOutput[StringType]("some-queue"))
      .build()

    pipeline.startMock()
  }
    **/
  //  test("All data can be read from mongo") {
  //    RMQStringCollectSink.reset()
  //
  //    val pipeline = new PipelineBuilder()
  //      .append(new RabbitMQInput[StringType]("some-queue"))
  //      .append({ x : DataStream[StringType] =>
  //        x.addSink(new RMQStringCollectSink).setParallelism(1)
  //      })
  //      .build()
  //
  //    pipeline.startMock()
  //
  //    val list = RMQStringCollectSink.asList
  //    val items = longString.split("[ \n]").toList
  //
  //    assert(items.containsAll(RMQStringCollectSink.result))
  //    assert(list.size == items.size)
  //    assert(RMQStringCollectSink.numEventTimes == 0)
  //  }

  test("Source should be properly bound to DataStream.") {
    val input = new RabbitMQInput[StringType]("")
    val output = new RabbitMQOutput[StringType]("")
    val stream = mock[DataStream[StringType]]
    val env = mock[StreamExecutionEnvironment]

    val context = pipeline.Context(env, "")

    input.main(context)
    verify(env).addSource(any[RMQSource[StringType]])(
      any[TypeInformation[StringType]])

    output.main(stream)
    verify(stream).addSink(any[RMQSinkDurable[StringType]])
  }
}

object RMQStringCollectSink {
  var result = new util.ArrayList[String]() //mutable list
  var numEventTimes = 0

  def reset(): Unit = {
    result = new util.ArrayList[String]()
    numEventTimes = 0
  }

  def asList: List[String] = result.asScala.toList
}

class RMQStringCollectSink extends SinkFunction[StringType] {

  override def invoke(value: StringType,
                      context: SinkFunction.Context[_]): Unit = {
    synchronized {
      RMQStringCollectSink.result.add(value.value)

      if (context.timestamp() != null) {
        RMQStringCollectSink.numEventTimes += 1
      }
    }
  }
}
