/*
 * Licensed to SenseML(http://senseml.org) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. SenseML licenses this file to You
 * under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
