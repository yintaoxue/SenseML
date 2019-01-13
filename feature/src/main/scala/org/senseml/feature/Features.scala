package org.senseml.feature

import org.apache.spark.sql.functions.{col, max, udf}
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.senseml.feature.features.{DateTimeFeature, StatFeature}
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


  def makeAggFeature(spark: SparkSession, df: DataFrame, groupby: List[String], fields: List[String]): DataFrame = {
    // make agg features
    val statDF = StatFeature.makeAggFeature(spark, df, groupby, fields)

    // join togethor
    val joinDF = df.join(statDF, groupby.toSeq, "left")
    joinDF
  }


}