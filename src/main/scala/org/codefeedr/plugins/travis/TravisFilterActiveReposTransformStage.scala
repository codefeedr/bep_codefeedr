package org.codefeedr.plugins.travis

import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.PipelineObject

class TravisFilterActiveReposTransformStage extends PipelineObject[GithubPushEventItem, GithubPushEventItem] {

  val travis: TravisService = new TravisService("su_9TrVO1Tbbti1UoG0Z_w")

  /**
    * Transforms the pipeline object from its input type to its output type.
    * This requires using the Flink DataStream API.
    *
    * @param source the input source.
    * @return the transformed stream.
    */
  override def transform(source: DataStream[GithubPushEventItem]): DataStream[GithubPushEventItem] = {
    source.filter(x => travis.repoIsActive(x.slug))
  }


}
