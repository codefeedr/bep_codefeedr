package org.codefeedr.plugins.pypi.stages

import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.Context
import org.codefeedr.plugins.pypi.operators.{
  PyPiReleasesSource,
  PyPiSourceConfig
}
import org.codefeedr.plugins.pypi.protocol.Protocol.PyPiRelease
import org.codefeedr.stages.InputStage
import org.codefeedr.stages.utilities.DefaultTypeMapper._

import scala.language.higherKinds

/** Fetches real-time releases from PyPi. */
class PyPiReleasesStage(stageId: String = "pypi_releases_min",
                        sourceConfig: PyPiSourceConfig = PyPiSourceConfig())
    extends InputStage[PyPiRelease](Some(stageId)) {

  /** Fetches [[PyPiRelease]] from real-time PyPi feed.
    *
    * @param context The context to add the source to.
    * @return The stream of type [[PyPiRelease]].
    */
  override def main(context: Context): DataStream[PyPiRelease] =
    context.env
      .addSource(new PyPiReleasesSource(sourceConfig))

  override def getSchema: Schema = {
    implicit val dateSchema: DateSchemaFor = new DateSchemaFor(true)
    AvroSchema[PyPiRelease]
  }
}
