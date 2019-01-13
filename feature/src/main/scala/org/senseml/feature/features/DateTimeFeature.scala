package org.senseml.feature.features

import java.util.{Calendar, Date}

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.senseml.feature.model.{Field, FeatureRow}
import org.senseml.feature.util.DateUtil

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * DateTimeFeature
  *
  * Created by xueyintao on 2019-01-11.
  */
object DateTimeFeature {

  val YEAR = new Field("year", "y", Int)
  val MONTH = new Field("month", "m", Int)
  val QUARTER = new Field("quarter", "q", Int)
  val WEEK = new Field("week", "w", Int)  // day of week
  val DAY = new Field("day", "d", Int)  // day of month
  val DAY_OF_YEAR = new Field("day_of_year", "d_of_y", Int)
  val WEEK_OF_MONTH = new Field("week_of_month", "w_of_m", Int)
  val WEEK_OF_YEAR = new Field("week_of_year", "w_of_y", Int)

  val HOUR = new Field("hour", "h", Int)
  val HOUR_QUARTER = new Field("hour_quarter", "h_q", Int)
  val HOUR_WORK = new Field("hour_work", "h_w", Int)  // between work hours


  var weekStartFromMonday = true
  var workHours = List(9, 18)


  /** DateTime Features */
  val dateFeatures = Array(YEAR, MONTH, QUARTER, WEEK, DAY, DAY_OF_YEAR, WEEK_OF_MONTH, WEEK_OF_YEAR)
  val timeFeatures = Array(HOUR, HOUR_QUARTER, HOUR_WORK)

  val features = Array.concat(dateFeatures, timeFeatures)


  def makeDateTimeFeature(spark: SparkSession, df: DataFrame, field: String, withTime: Boolean = true): DataFrame = {
    // UDF func to make datatime features
    def funDTFeature(): String => Array[Int] = {
      x =>
        val dt = DateUtil.parseDateTime(x)
        make(dt, withTime)
    }

    val udfDTFeature = udf(funDTFeature())

    val newFieldName = field + "__dt"
    val dfWithDT = df.withColumn(newFieldName, udfDTFeature(col(field)))

    val fieldNames = getFieldNames(withTime)

    // unpack columns
    var colList = ListBuffer[Column]()
    colList += col("*")
    for (i <- fieldNames.indices) {
      colList += col(newFieldName)(i).alias(fieldNames(i))
    }

    // unpack
    dfWithDT.select(colList: _*).drop(newFieldName)
  }

  /**
    * make date features
    *
    * @param date
    * @param withTime only date or has time, decide whether make time features
    * @return
    */
  def make(date: Date, withTime: Boolean = true): Array[Int] = {
    if (withTime)
      make(date, features)
    else
      make(date, dateFeatures)
  }

  /**
    * make given features
    *
    * @param date Date
    * @param fields feature fields
    * @return
    */
  def make(date: Date, fields: Array[Field]): Array[Int] = {
    val values = new ArrayBuffer[Int]()
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.setMinimalDaysInFirstWeek(7)

    for (f <- fields) {
      var rs = f.name match {
        case "year" => cal.get(Calendar.YEAR)
        case "month" => cal.get(Calendar.MONTH) + 1
        case "quarter" => (cal.get(Calendar.MONTH) + 3) / 3
        case "week" =>
          if (weekStartFromMonday) {  // week start from monday
            var week = cal.get(Calendar.DAY_OF_WEEK)
            week -= 1
            if (week <= 0) 7 else week
          }
          else cal.get(Calendar.DAY_OF_WEEK)
        case "day" => cal.get(Calendar.DAY_OF_MONTH)
        case "day_of_year" => cal.get(Calendar.DAY_OF_YEAR)
        case "week_of_month" => cal.get(Calendar.WEEK_OF_MONTH)
        case "week_of_year" => cal.get(Calendar.WEEK_OF_YEAR)

        case "hour" => cal.get(Calendar.HOUR_OF_DAY)
        case "hour_quarter" => cal.get(Calendar.HOUR_OF_DAY) / 6 + 1
        case "hour_work" =>
          if (cal.get(Calendar.HOUR_OF_DAY) >= workHours(0) && cal.get(Calendar.HOUR_OF_DAY) < workHours(1))
            1
          else 0
        case _ => 0
      }
      values += rs
    }

    values.toArray
  }

  def getFieldNames(withTime: Boolean = true): Array[String] = {
    if (withTime)
      features.map(_.name)
    else
      dateFeatures.map(_.name)
  }

}
