package org.senseml.feature

import org.apache.spark.sql.functions.{col, max, udf}
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.senseml.feature.features.DateTimeFeature
import org.senseml.feature.util.DateUtil

import scala.collection.mutable.ListBuffer

/**
  * Features
  * Created by xueyintao on 2019-01-11.
  */
object Features {

  def makeDateTimeFeature(spark: SparkSession, df: DataFrame, field: String): DataFrame = {

    def funDTFeature(): String => Array[Int] = {
      x =>
        val dt = DateUtil.parseDateTime(x)
        DateTimeFeature.make(dt, false).value.toArray
    }

    val udfDTFeature = udf(funDTFeature)

    val rs = df.withColumn(field + "_dt", udfDTFeature(col(field)))
    rs
  }


  def makeAggFeature(spark: SparkSession, df: DataFrame, groupBy: String, fields: String): DataFrame = {
    val aggFuncs = "sum,avg,count,min,max"
    import org.apache.spark.sql.functions._

    // generate agg funcs
    var funcList = ListBuffer[Column]()
    for (field <- fields.split(","))
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

    val gb = df.groupBy(groupBy)
    val aggRS = gb.agg(funcList.head, funcList.tail: _*)
    aggRS
  }


}