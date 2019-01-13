package org.senseml.feature

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.senseml.feature.features.{DateTimeFeature, StatFeature}

/**
  * Features
  * Created by xueyintao on 2019-01-11.
  */
object Features {


  def makeDateTimeFeature(spark: SparkSession, df: DataFrame, field: String, withTime: Boolean = true): DataFrame = {
    DateTimeFeature.makeDateTimeFeature(spark, df, field, withTime)
  }

  def makeAggFeature(spark: SparkSession, df: DataFrame, groupby: List[String], fields: List[String]): DataFrame = {
    // make agg features
    val statDF = StatFeature.makeAggFeature(spark, df, groupby, fields)

    // join togethor
    val joinDF = df.join(statDF, groupby.toSeq, "left")
    joinDF
  }


}