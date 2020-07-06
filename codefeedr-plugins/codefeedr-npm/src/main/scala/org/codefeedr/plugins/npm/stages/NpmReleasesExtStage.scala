package org.codefeedr.plugins.npm.stages

import java.util.concurrent.TimeUnit

import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema
import org.apache.flink.streaming.api.datastream.{
  AsyncDataStream => JavaAsyncDataStream
}
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.plugins.npm.operators.RetrieveProjectAsync
import org.codefeedr.plugins.npm.protocol.Protocol.{NpmRelease, NpmReleaseExt}
import org.codefeedr.stages.TransformStage
import org.codefeedr.stages.utilities.DefaultTypeMapper.DateSchemaFor

import scala.language.higherKinds

/** Transform a [[NpmRelease]] to [[NpmReleaseExt]].
  *
  * @param stageId the name of this stage.
  *
  * @author Roald van der Heijden
  * Date: 2019-12-01 (YYYY-MM-DD)
  */
class NpmReleasesExtStage(stageId: String = "npm_releases")
    extends TransformStage[NpmRelease, NpmReleaseExt](Some(stageId)) {

  /**
    * Transform a [[NpmRelease]] to [[NpmReleaseExt]].
    *
    * @param source The input source with type [[NpmRelease]].
    * @return The transformed stream with type [[NpmReleaseExt]].
    */
  override def transform(
      source: DataStream[NpmRelease]): DataStream[NpmReleaseExt] = {

    // Retrieve project from release asynchronously.
    val async = JavaAsyncDataStream.orderedWait(source.javaStream,
                                                new RetrieveProjectAsync,
                                                5,
                                                TimeUnit.SECONDS,
                                                100)
    /*
  for testing purposes this is commmented out, but until we merge into master at the end of the project
  please leave these lines commented!!
     */

    // implicit val typeInfo = TypeInformation.of(classOf[NpmReleaseExt])
//    new org.apache.flink.streaming.api.scala.DataStream(async)
//          .map(x => NpmReleaseExt(x.name, x.retrieveDate, x.project))
//          .print()

    new org.apache.flink.streaming.api.scala.DataStream(async)
  }

  override def getSchema: Schema = {
    implicit val dateSchema: DateSchemaFor = new DateSchemaFor(true)
    AvroSchema[NpmReleaseExt]
  }
}
