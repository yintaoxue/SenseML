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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.senseml.feature.util.DataFrameUtil

/**
  * PivotFeature
  *
  * Author: YintaoXue (ruogu.org)
  * Date: 2019-01-15
  */
object PivotFeature {

  /**
    * make pivot features
    *
    * @param spark SparkSession
    * @param df DataFrame
    * @param groupby group by field
    * @param pivot pivot field
    * @param pivotValues pivot values, can be null
    * @param fields fields to agg
    * @param aggFuncs agg funcs
    * @return
    */
  def makePivotFeature(spark: SparkSession, df: DataFrame, groupby: List[String], pivot: String, pivotValues: List[Any],
                       fields: List[String], aggFuncs: List[String]): DataFrame = {
    // generate agg funcs
    val funcList = DataFrameUtil.makeAggFuncs(fields, aggFuncs)

    // groupby agg
    var resultDF: DataFrame = null

    if (pivotValues == null)
      resultDF = df.groupBy(groupby.head, groupby.tail: _*).pivot(pivot).agg(funcList.head, funcList.tail: _*)
    else
        resultDF = df.groupBy(groupby.head, groupby.tail: _*).pivot(pivot, pivotValues).agg(funcList.head, funcList.tail: _*)
    resultDF
  }

}
