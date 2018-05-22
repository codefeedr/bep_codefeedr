package org.codefeedr.plugins.travis

import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.TransformStage

class TravisPushEventBuildInfoTransformStage extends TransformStage[GithubPushEventItem, TravisBuild]{

  val travis: TravisService = new TravisService()

  override def transform(source: DataStream[GithubPushEventItem]): DataStream[TravisBuild] = {
    null
  }

}
