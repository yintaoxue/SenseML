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
package org.senseml.feature.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * DataFrameUtil
  *
  * Author: YintaoXue (ruogu.org)
  * Date: 2019-01-15
  */
object DataFrameUtil {

  def makeAggFuncs(fields: List[String], aggFuncs: List[String]): ListBuffer[Column] = {
    var funcList = ListBuffer[Column]()
    for (field <- fields)
      for (func <- aggFuncs) {
        val funcf = func match {
          case "sum" => sum(field)
          case "avg" => avg(field)
          case "count" => count(field)
          case "min" => min(field)
          case "max" => max(field)
        }
        funcList += funcf
      }
    funcList
  }

}
