package org.senseml.feature.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * DateUtil
  *
  * Created by xueyintao on 2019-01-11.
  */
object DateUtil {

  val DATE_TIME_FMT = "yyyy-MM-dd HH:mm:ss"
  val DATE_FMT = "yyyy-MM-dd"

  def parseDateTime(date: String): Date = {
    val sdf = new SimpleDateFormat(DATE_TIME_FMT)
    sdf.parse(date)
  }

  def parseDate(date: String): Date = {
    val sdf = new SimpleDateFormat(DATE_FMT)
    sdf.parse(date)
  }

  def parseDate(date: String, fmt: String): Date = {
    val sdf = new SimpleDateFormat(fmt)
    sdf.parse(date)
  }

}
