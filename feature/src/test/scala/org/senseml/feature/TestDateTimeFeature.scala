package org.senseml.feature


import java.util.Calendar

import org.senseml.feature.DateTimeFeature._
import org.senseml.feature.util.DateUtil

/**
  * TestDateTimeFeature
  *
  * Created by xueyintao on 2019-01-11.
  */
object TestDateTimeFeature {

  def testMake(): Unit = {
    val dt = DateUtil.parseDateTime("2018-12-11 18:50:01")

    val features = Array(YEAR, MONTH, QUARTER, WEEK, DAY, DAY_OF_YEAR, WEEK_OF_MONTH, WEEK_OF_YEAR)
    val rs = DateTimeFeature.make(dt)
    println(rs)

  }

  def main(args: Array[String]): Unit = {

    testMake()
  }

}
