package org.senseml.feature.model

import scala.collection.mutable.ArrayBuffer

/**
  * Row
  *
  * Created by xueyintao on 2019-01-11.
  */
@SerialVersionUID(1L)
class Row[T] extends Serializable {

  var fields = new ArrayBuffer[Field]()
  var value = new ArrayBuffer[T]()

  def getFields(): ArrayBuffer[Field] = {
    this.fields
  }

  def getValues(): ArrayBuffer[T] = {
    this.value
  }

  override def toString = {
    var rs = new StringBuilder()
    rs.append("{")
    for (i <- 0 to fields.length) {
      rs.append(fields(i).name).append(":").append(value(i)).append(",")
    }
    if (rs.endsWith(","))
      rs.dropRight(1)
  }
}
