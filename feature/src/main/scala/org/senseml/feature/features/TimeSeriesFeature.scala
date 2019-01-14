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
package org.senseml.feature.features

import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.senseml.feature.util.DateUtil

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  * TimeSeriesFeature
  *
  * Author: YintaoXue (ruogu.org)
  * Date: 2019-01-14
  */
object TimeSeriesFeature {

  def makeTimeSeriesFeature(spark: SparkSession, df: DataFrame, dtField: String, calFields: List[String],
                            funcs: String, startDT: Date, dtWindows: List[Int], backward: Boolean = true): DataFrame = {
    import spark.implicits._

    // trans date to window id
    val dtValues = df.select(dtField).distinct()

    //
    def funcDateWindow(): Date => String = {
      date =>
        var diffDays = DateUtil.diffDays(startDT, date)
        if (!backward)
          diffDays *= -1

        // diff days windows
        var start = 0
        var end = 0
        var windArray = ArrayBuffer[Array[Int]]()
        var cnt = 0
        for (t <- dtWindows) {
          start = cnt
          end = start + t - 1
          cnt += t
          windArray += Array(start, end)
        }

        // find date's window id in dtWindows
        var wind = -1
        breakable {
          for (i <- windArray) {
            wind += 1
            if (diffDays >= i(0) && diffDays <= i(1)) {
              break
            }
          }
        }

        // date in window:$wind, [start,end]
        "ts_" + start + "_" + end
    }

    val udfDateWindow = udf(funcDateWindow())

    val newFieldName = "dtField" + "__ts"
    val dtWindowDF = dtValues.withColumn(newFieldName, udfDateWindow(col(dtField)))

    // join
    val resultDF = df.join(dtWindowDF, df(dtField) === dtWindowDF(dtField), "inner")
    resultDF
  }

}
