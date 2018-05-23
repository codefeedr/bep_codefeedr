package org.codefeedr.plugins.travis

import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.codefeedr.pipeline.PipelineObject

import scala.concurrent.{ExecutionContext, Future}

class TravisGetFinishedBuildTransformStage extends PipelineObject[TravisBuild, TravisBuild] {

  override def transform(source: DataStream[TravisBuild]): DataStream[TravisBuild] = {
//    source.
    null
  }

}

class AsyncDatabaseRequest extends AsyncFunction[String, (String, String)] {

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())


  override def asyncInvoke(str: String, resultFuture: ResultFuture[(String, String)]): Unit = {

    // issue the asynchronous request, receive a future for the result
    //    val resultFutureRequested: Future[String] = client.query(str)

    // set the callback to be executed once the request by the client is complete
    // the callback simply forwards the result to the result future
    //    resultFutureRequested.onSuccess {
    //      case result: String => resultFuture.complete(Iterable((str, result)))
    //    }
  }
}