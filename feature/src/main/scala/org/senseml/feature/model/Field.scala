package org.senseml.feature.model

/**
  * Field
  *
  * Created by xueyintao on 2019-01-11.
  */
class Field {
  // field name
  var name: String = _
  // simple field name
  var simple: String = _
  // field type, like Int, Double
  var dtype: Object = _
  // field value
  var value: Object = _
  // value type: VTYPE_NUM(default), VTYPE_INDEX, VTYPE_DIM, VTYPE_CATEGORY
  var vtype: Int = 0

  def this(name: String, simple: String, dtype: Object, value: Object = null, vtype: Int = 0) {
    this()
    this.name = name
    this.simple = simple
    this.dtype = dtype
    this.value = value
    this.vtype = vtype
  }

  override def toString = s"Field($name, $simple, $dtype, $value, $vtype)"
}
