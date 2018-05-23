package org.codefeedr.plugins.travis

import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.{Pipeline, PipelineObject}
import org.codefeedr.plugins.github.GitHubProtocol.PushEvent

class TravisFilterActiveReposTransformStage extends PipelineObject[PushEvent, PushEvent] {

  var travis: TravisService = _

  override def setUp(pipeline: Pipeline): Unit = {
    super.setUp(pipeline)
    travis = new TravisService(pipeline.keyManager)
  }

  /**
    * Transforms the pipeline object from its input type to its output type.
    * This requires using the Flink DataStream API.
    *
    * @param source the input source.
    * @return the transformed stream.
    */
  override def transform(source: DataStream[PushEvent]): DataStream[PushEvent] = {
    source.filter(x => travis.repoIsActive(x.repo.name))
  }


}
