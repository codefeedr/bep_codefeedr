package org.codefeedr.plugins.maven.stages

import java.util
import java.util.Date

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.PipelineBuilder
import org.scalatest.FunSuite
import org.codefeedr.plugins.maven.protocol.Protocol.{Guid, MavenRelease, MavenReleaseExt}

class MavenReleasesExtStageTest extends FunSuite {

  //TODO this test now pulls an existing project, mocking is better though since we now rely on external sources
  //TODO the order of these tests now matters, find out why

  test("MavenReleasesExtStage integration empty project test"){
    val release = MavenRelease("org.neo4j.ERROR: it-test-support 3.5.13", "url", "desc", new Date(), Guid("tag"))
    implicit val typeInfo: TypeInformation[MavenRelease] = TypeInformation.of(classOf[MavenRelease])


    new PipelineBuilder()
      .appendSource(x => x.fromCollection(List(release))(typeInfo))
      .append(new MavenReleasesExtStage())
      .append { x: DataStream[MavenReleaseExt] =>
        x.addSink(new CollectReleases)
      }
      .build()
      .startMock()

    assert(CollectReleases.result.size() == 0)
  }

  test("MavenReleasesExtStage integration test"){
    val release = MavenRelease("org.neo4j.community: it-test-support 3.5.13", "url", "desc", new Date(), Guid("tag"))
    implicit val typeInfo: TypeInformation[MavenRelease] = TypeInformation.of(classOf[MavenRelease])


    new PipelineBuilder()
      .appendSource(x => x.fromCollection(List(release))(typeInfo))
      .append(new MavenReleasesExtStage())
      .append { x: DataStream[MavenReleaseExt] =>
        x.addSink(new CollectReleases)
      }
      .build()
      .startMock()

    assert(CollectReleases.result.size() == 1)
  }

}

object CollectReleases {
  val result = new util.ArrayList[MavenReleaseExt]()
}

class CollectReleases extends SinkFunction[MavenReleaseExt] {
  override def invoke(value: MavenReleaseExt,
                      context: SinkFunction.Context[_]): Unit = {
    CollectReleases.result.add(value)
  }
}