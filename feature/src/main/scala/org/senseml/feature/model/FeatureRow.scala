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
