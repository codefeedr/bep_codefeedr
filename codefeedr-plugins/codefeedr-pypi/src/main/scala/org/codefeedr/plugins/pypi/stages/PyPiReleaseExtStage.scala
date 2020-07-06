package org.codefeedr.plugins.pypi.stages

import java.util.concurrent.TimeUnit

import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.datastream.{
  AsyncDataStream => JavaAsyncDataStream
}
import org.codefeedr.plugins.pypi.protocol.Protocol.{
  PyPiRelease,
  PyPiReleaseExt
}
import org.codefeedr.stages.TransformStage
import org.codefeedr.plugins.pypi.operators.RetrieveProjectAsync
import org.codefeedr.stages.utilities.DefaultTypeMapper.DateSchemaFor

import scala.language.higherKinds

/** Transform a [[PyPiRelease]] to [[PyPiReleaseExt]].
  *
  * @param stageId the name of this stage.
  */
class PyPiReleaseExtStage(stageId: String = "pypi_releases")
    extends TransformStage[PyPiRelease, PyPiReleaseExt](Some(stageId)) {

  /** Transform a [[PyPiRelease]] to [[PyPiReleaseExt]].
    *
    * @param source The input source with type [[PyPiRelease]].
    * @return The transformed stream with type [[PyPiReleaseExt]].
    */
  override def transform(
      source: DataStream[PyPiRelease]): DataStream[PyPiReleaseExt] = {

    /** Retrieve project from release asynchronously. */
    val async = JavaAsyncDataStream.orderedWait(source.javaStream,
                                                new RetrieveProjectAsync,
                                                5,
                                                TimeUnit.SECONDS,
                                                100)

    new org.apache.flink.streaming.api.scala.DataStream(async)
  }

  override def getSchema: Schema = {
    implicit val dateSchema: DateSchemaFor = new DateSchemaFor(true)
    AvroSchema[PyPiReleaseExt]
  }
}
