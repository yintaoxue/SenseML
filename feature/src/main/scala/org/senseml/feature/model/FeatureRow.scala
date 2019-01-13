package org.senseml.feature.model

import scala.collection.mutable.ArrayBuffer

/**
  * Row
  *
  * Created by xueyintao on 2019-01-11.
  */
@SerialVersionUID(1L)
class FeatureRow[T] extends Serializable {

  var fields = new ArrayBuffer[Field]()
  var value = new ArrayBuffer[T]()

  override def toString(): String = {
    var rs = new StringBuilder()
    rs.append("{")
    for (i <- fields.indices) {
      rs.append(fields(i).name).append(":").append(value(i)).append(",")
    }
    if (rs.endsWith(","))
      rs = rs.dropRight(1)
    rs.append("}")
    rs.toString()
  }
}
