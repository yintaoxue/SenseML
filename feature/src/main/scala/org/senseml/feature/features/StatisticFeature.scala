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
  * StatisticFeature
  *
  * Created by xueyintao on 2019-01-13.
  */
object StatisticFeature {

  val defaultAggFuncs = List("sum", "avg", "count", "min" , "max")

  /**
    * make groupby agg features, support:sum,avg,count,min,max
    *
    * @param spark SparkSession
    * @param df DataFrame
    * @param groupby List of groupby field
    * @param fields fields to appply agg func
    * @return
    */
  def makeAggFeature(spark: SparkSession, df: DataFrame, groupby: List[String], fields: List[String]): DataFrame = {
    makeAggFeature(spark, df, groupby, fields, defaultAggFuncs)
  }

  /**
    * make groupby agg features, support:sum,avg,count,min,max
    *
    * @param spark SparkSession
    * @param df DataFrame
    * @param groupby List of groupby field
    * @param fields fields to appply agg func
    * @param aggFuncs agg functions, comma separated
    * @return
    */
  def makeAggFeature(spark: SparkSession, df: DataFrame, groupby: List[String], fields: List[String], aggFuncs: List[String]): DataFrame = {
    // make agg features
    val statDF = groupbyAggFeature(spark, df, groupby, fields, aggFuncs)

    // join togethor
    val joinDF = df.join(statDF, groupby.toSeq, "left")
    joinDF
  }

  def groupbyAggFeature(spark: SparkSession, df: DataFrame, groupby: List[String], fields: List[String], aggFuncs: List[String]): DataFrame = {
    // generate agg funcs
    val funcList = DataFrameUtil.makeAggFuncs(fields, aggFuncs)

    // groupby agg
    val gb = df.groupBy(groupby.head, groupby.tail: _*)
    val aggRS = gb.agg(funcList.head, funcList.tail: _*)
    aggRS
  }

}
