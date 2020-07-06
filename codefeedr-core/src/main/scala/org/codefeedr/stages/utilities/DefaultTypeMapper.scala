package org.codefeedr.stages.utilities

import java.util.Date

import com.sksamuel.avro4s._
import org.apache.avro.Schema

object DefaultTypeMapper {

  /**
    * Generate an Avro Schema for java.util.Date.
    *
    * @param isRowtime determines if Date field is used as a rowtime attribute
    */
  class DateSchemaFor(val isRowtime: Boolean = false) extends SchemaFor[Date] {

    override def schema(fieldMapper: FieldMapper): Schema = {
      val sc = Schema.create(Schema.Type.STRING)
      if (isRowtime) {
        sc.addProp("isRowtime", isRowtime)
      }
      sc
    }
  }

}
