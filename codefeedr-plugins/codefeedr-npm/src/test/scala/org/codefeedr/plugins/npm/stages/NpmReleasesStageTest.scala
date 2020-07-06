package org.codefeedr.plugins.npm.stages

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.PipelineBuilder
import org.codefeedr.stages.OutputStage
import org.scalatest.FunSuite
import org.codefeedr.plugins.npm.operators.NpmSourceConfig
import org.codefeedr.plugins.npm.protocol.Protocol.NpmRelease

/**
 * Class to test NpmReleasesStage class
 *
 * @author Roald van der Heijden
 * Date: 2019-12-01 (YYYY-MM-DD)
 */
class NpmReleasesStageTest extends FunSuite {
  test("NpmReleasesIntegrationTest") {
    val source = new NpmReleasesStage(sourceConfig = NpmSourceConfig(1000,12,8))
    val sink = new LimitingSinkStage(4)

    val pipeline: Unit = new PipelineBuilder()
      .append(source)
      .append(sink)
      .build()
      .startMock()
  }
}

/**
 * Simple Sink Pipeline Object that limits the output to a certain number
 * and is able to get a list of all the items that were received in the sink
 * Adapted from:  W.R. Zonneveld's Maven plugin for CodeFeedr
 *
 * @author Roald van der Heijden
 * Date: 2019-12-01
 */
class LimitingSinkStage(elements: Int = -1)
  extends OutputStage[NpmRelease]
    with Serializable {
  var sink: LimitingSink = _

  override def main(source: DataStream[NpmRelease]): Unit = {
    sink = new LimitingSink(elements)
    source.addSink(sink).setParallelism(1)
  }
}

/**
 * Adapted from:  W.R. Zonneveld's Maven plugin for CodeFeedr
 */
class LimitingSink(elements: Int) extends SinkFunction[NpmRelease] {
  var count = 0
  var items: List[NpmRelease] = List()

  override def invoke(value: NpmRelease,
                      context: SinkFunction.Context[_]): Unit = {
    count += 1
    items = value :: items

    //println(count) // annoyed by this during testing, commented out

    if (elements != -1 && count >= elements) {
      throw new RuntimeException()
    }
  }
}