package org.codefeedr.plugins.travis.stages

import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.codefeedr.pipeline.{Pipeline, PipelineObject}
import org.codefeedr.plugins.github.GitHubProtocol.PushEvent
import org.codefeedr.plugins.travis.TravisProtocol.PushEventFromActiveTravisRepo
import org.codefeedr.plugins.travis.util.TravisService

class TravisFilterActiveReposTransformStage extends PipelineObject[PushEvent, PushEventFromActiveTravisRepo] {

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
  override def transform(source: DataStream[PushEvent]): DataStream[PushEventFromActiveTravisRepo] = {

    val filter = travis.repoIsActiveFilter

    source
      .filter(x => filter(x.repo.name))
      .map(PushEventFromActiveTravisRepo)
  }
}
