package org.senseml.feature.model

/**
  * Field
  *
  * Created by xueyintao on 2019-01-11.
  */
class Field {
  // 字段名
  var name: String = _
  // 字段缩写
  var simple: String = _
  // 字段类型
  var dtype: Object = _
  // 字段值
  var value: Object = null

  def this(name: String, simple: String, dtype: Object, value: Object = null) {
    this()
    this.name = name
    this.simple = simple
    this.dtype = dtype
    this.value = value
  }

  override def toString = s"Field($name, $simple, $dtype, $value)"
}
