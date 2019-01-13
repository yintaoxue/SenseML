package org.senseml.feature.features

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * StatFeature
  * statistical features
  *
  * Created by xueyintao on 2019-01-13.
  */
object StatFeature {

  val defaultAggFuncs = "sum,avg,count,min,max"
  import org.apache.spark.sql.functions._

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
  def makeAggFeature(spark: SparkSession, df: DataFrame, groupby: List[String], fields: List[String], aggFuncs: String): DataFrame = {
    // generate agg funcs
    var funcList = ListBuffer[Column]()
    for (field <- fields)
      for (func <- aggFuncs.split(",")) {
        val funcf = func match {
          case "sum" => sum(field)
          case "avg" => avg(field)
          case "count" => count(field)
          case "min" => min(field)
          case "max" => max(field)
        }
        funcList += funcf
      }

    // groupby agg
    val gb = df.groupBy(groupby.head, groupby.tail: _*)
    val aggRS = gb.agg(funcList.head, funcList.tail: _*)
    aggRS
  }

}
