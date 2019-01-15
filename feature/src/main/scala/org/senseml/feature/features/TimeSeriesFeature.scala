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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.senseml.feature.util.DateUtil
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

/**
  * TimeSeriesFeature
  *
  * Author: YintaoXue (ruogu.org)
  * Date: 2019-01-14
  */
object TimeSeriesFeature {

  /**
    * make time series features, dtWindows are aggregate into rows
    *
    * @param spark SparkSession
    * @param df dataset
    * @param dtField the date field name to calculate date window with
    * @param calFields the fields to make sum/count etc
    * @param funcs sum,count...ops to make on calFields
    * @param startDT start date to calculate the window
    * @param dtWindows date windows, it's day ranges
    * @param backward date add backward(default) or forward
    * @return
    */
  def makeTimeSeriesFeature(spark: SparkSession, df: DataFrame, dtField: String, groupby: List[String], calFields: List[String],
                            funcs: List[String], startDT: Date, dtWindows: List[Int], backward: Boolean = true): DataFrame = {
    // trans date to window id
    val dtValues = df.select(dtField).distinct()

    def funcDateWindow(): Date => String = {
      date =>
        var diffDays = DateUtil.diffDays(startDT, date)
        if (!backward)
          diffDays *= -1

        // diff days windows
        val windArray = getWindArray(dtWindows)

        // find date's window id in dtWindows
        var wind = -1
        var start = -1
        var end = -1
        breakable {
          for (i <- windArray) {
            wind += 1
            if (diffDays >= i(0) && diffDays <= i(1)) {
              start = i(0)
              end = i(1)
              break
            }
          }
        }
        if (start == -1)
          wind = -1

        // date in window:$wind, [start,end]
        s"ts_${wind}_${start}_${end}"
    }

    val udfDateWindow = udf(funcDateWindow())

    val windFieldName = dtField + "__ts"
    val dtWindowDF = dtValues.withColumn(windFieldName, udfDateWindow(col(dtField))).filter(col(windFieldName) =!= "ts_-1_-1_-1")
    // join
    val joinedDF = df.join(dtWindowDF, List(dtField), "inner")

    // groupby time window
    val groupbyList = ListBuffer[String]()
    groupbyList ++= groupby
    groupbyList += windFieldName
    val result = StatisticFeature.groupbyAggFeature(spark, joinedDF, groupbyList.toList, calFields, funcs)

    result
  }

  /**
    * Trans dtWindow to date intervals
    *
    * @param dtWindows
    * @return
    */
  def getWindArray(dtWindows: List[Int]): ArrayBuffer[Array[Int]] = {
    // diff days windows
    var windArray = ArrayBuffer[Array[Int]]()
    var cnt = 0
    for (t <- dtWindows) {
      val start = cnt
      val end = start + t - 1
      cnt += t
      windArray += Array(start, end)
    }
    windArray
  }


  /**
    * Get start,end date by dtWindows
    *
    * @param startDT
    * @param dtWindows
    * @param backward
    * @return dateRange
    */
  def getDateRangeByWindow(startDT: Date, dtWindows: List[Int], backward: Boolean = true): ArrayBuffer[Array[Date]] = {
    val windArray = getWindArray(dtWindows)
    var dateRange = ArrayBuffer[Array[Date]]()

    for (dt <- windArray) {
      val flag = if (backward) -1 else 1
      val start = DateUtil.addDate(startDT, dt(0) * flag)
      val end = DateUtil.addDate(startDT, dt(1) * flag)
      dateRange += Array(start, end)
    }

    dateRange
  }

}
