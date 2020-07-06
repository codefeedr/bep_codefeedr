package org.codefeedr.plugins.maven.stages

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.PipelineBuilder
import org.codefeedr.stages.OutputStage
import org.scalatest.FunSuite
import org.codefeedr.plugins.maven.operators.MavenSourceConfig
import org.codefeedr.plugins.maven.protocol.Protocol.MavenRelease

class MavenReleasesStageTest extends FunSuite {

  test("MavenReleasesIntegrationTest") {
    val source = new MavenReleasesStage(sourceConfig = MavenSourceConfig(1000, 12, 4))
    val sink = new LimitingSinkStage(4)

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
  extends OutputStage[MavenRelease]
    with Serializable {
  var sink: LimitingSink = _

  override def main(source: DataStream[MavenRelease]): Unit = {
    sink = new LimitingSink(elements)
    source.addSink(sink).setParallelism(1)
  }
}

class LimitingSink(elements: Int) extends SinkFunction[MavenRelease] {
  var count = 0
  var items: List[MavenRelease] = List()

  override def invoke(value: MavenRelease,
                      context: SinkFunction.Context[_]): Unit = {
    count += 1
    items = value :: items

    println(count)

    if (elements != -1 && count >= elements) {
      throw new RuntimeException()
    }
  }
}
