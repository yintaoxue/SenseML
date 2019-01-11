package org.senseml.feature.model

import scala.collection.mutable.ArrayBuffer

/**
  * Schema
  *
  * Created by xueyintao on 2019-01-11.
  */
class Schema {

  // 字段名列表
  var names = new ArrayBuffer[String]()

  // 字段类型列表
  var types = new ArrayBuffer[Object]()

}
