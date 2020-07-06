package org.codefeedr.plugins.maven.stages

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.datastream.{
  AsyncDataStream => JavaAsyncDataStream
}
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.stages.TransformStage
import org.codefeedr.plugins.maven.operators.RetrieveProjectAsync
import org.codefeedr.plugins.maven.protocol.Protocol.{
  MavenRelease,
  MavenReleaseExt
}

/** Transform a [[MavenRelease]] to [[MavenReleaseExt]].
  *
  * @param stageId the name of this stage.
  */
class MavenReleasesExtStage(stageId: String = "maven_releases")
    extends TransformStage[MavenRelease, MavenReleaseExt](Some(stageId)) {

  /** Transform a [[MavenRelease]] to [[MavenReleaseExt]].
    *
    * @param source The input source with type [[MavenRelease]].
    * @return The transformed stream with type [[MavenReleaseExt]].
    */
  override def transform(
      source: DataStream[MavenRelease]): DataStream[MavenReleaseExt] = {

    /** Retrieve project from release asynchronously. */
    val async = JavaAsyncDataStream.orderedWait(source.javaStream,
                                                new RetrieveProjectAsync,
                                                5,
                                                TimeUnit.SECONDS,
                                                100)

    new org.apache.flink.streaming.api.scala.DataStream(async)
  }
}
