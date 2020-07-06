package org.codefeedr.plugins.clearlydefined.stages

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.PipelineBuilder
import org.codefeedr.stages.OutputStage
import org.scalatest.FunSuite
import org.codefeedr.plugins.clearlydefined.operators.ClearlyDefinedSourceConfig
import org.codefeedr.plugins.clearlydefined.protocol.Protocol.ClearlyDefinedRelease

class ClearlyDefinedReleasesStageTests extends FunSuite {

  test("ClearlyDefinedReleasesIntegrationTest") {
    val source = new ClearlyDefinedReleasesStage(sourceConfig = ClearlyDefinedSourceConfig(1000, 10, 4))
    val sink = new LimitingSinkStage(10)

    val pipeline: Unit = new PipelineBuilder()
      .append(source)
      .append(sink)
      .build()
      .startMock()
  }
}

// Simple Sink Pipeline Object that limits the output to a certain number
// and is able to get a list of all the items that were received in the sink
class LimitingSinkStage(elements: Int = -1)
  extends OutputStage[ClearlyDefinedRelease]
    with Serializable {
  var sink: LimitingSink = _

  override def main(source: DataStream[ClearlyDefinedRelease]): Unit = {
    sink = new LimitingSink(elements)
    source.addSink(sink).setParallelism(1)
  }
}

class LimitingSink(elements: Int) extends SinkFunction[ClearlyDefinedRelease] {
  var count = 0
  var items: List[ClearlyDefinedRelease] = List()

  override def invoke(value: ClearlyDefinedRelease,
                      context: SinkFunction.Context[_]): Unit = {
    count += 1
    items = value :: items

    println(count)

    if (elements != -1 && count >= elements) {
      throw new RuntimeException()
    }
  }

}

