package org.senseml.feature

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.senseml.feature.features.DateTimeFeature
import org.senseml.feature.util.DateUtil

/**
  * Features
  * Created by xueyintao on 2019-01-11.
  */
class Features {



}

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

}