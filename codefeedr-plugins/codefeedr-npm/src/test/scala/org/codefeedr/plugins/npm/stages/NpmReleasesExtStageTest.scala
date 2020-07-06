package org.codefeedr.plugins.npm.stages

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.PipelineBuilder
import org.scalatest.FunSuite
import org.codefeedr.plugins.npm.protocol.Protocol.{NpmRelease, NpmReleaseExt}

/**
 * Integration tests for ReleaseExtStage
 *
 * Adapted from W.R. Zonnveld's Maven plugin tests
 *
 * @author Roald van der Heijden
 * Date: 2019-12-01
 */
class NpmReleasesExtStageTest extends FunSuite {

  test("NpmReleasesExtStage integration empty project test"){
    val now = Calendar.getInstance().getTime
    val sdf = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss Z")
    sdf.format(now)

    val release = NpmRelease("nonexistingpackageRoald", now)
    implicit val typeInfo: TypeInformation[NpmRelease] = TypeInformation.of(classOf[NpmRelease])

    new PipelineBuilder()
      .appendSource(x => x.fromCollection(List(release))(typeInfo))
      .append(new NpmReleasesExtStage())
      .append { x: DataStream[NpmReleaseExt] =>
        x.addSink(new CollectReleases)
      }
      .build()
      .startMock()

    assert(CollectReleases.result.size() == 0)
  }

  test("NpmReleasesExtStage integration test"){
    val now = Calendar.getInstance().getTime
    val sdf = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss Z")
    sdf.format(now)

    val release = NpmRelease("@microservices/cli", now)
    implicit val typeInfo: TypeInformation[NpmRelease] = TypeInformation.of(classOf[NpmRelease])

    new PipelineBuilder()
      .appendSource(x => x.fromCollection(List(release))(typeInfo))
      .append(new NpmReleasesExtStage())
      .append { x: DataStream[NpmReleaseExt] =>
        x.addSink(new CollectReleases)
      }
      .build()
      .startMock()

    assert(CollectReleases.result.size() == 1)
  }

}

/**
 * Adapted from W.R. Zonnveld's Maven plugin tests
 * Date: 2019-12-01
 */
object CollectReleases {
  val result = new util.ArrayList[NpmReleaseExt]()
}

/**
 * Adapted from W.R. Zonnveld's Maven plugin tests
 * Date: 2019-12-01
 */
class CollectReleases extends SinkFunction[NpmReleaseExt] {
  override def invoke(value: NpmReleaseExt,
                      context: SinkFunction.Context[_]): Unit = {
    CollectReleases.result.add(value)
  }
}