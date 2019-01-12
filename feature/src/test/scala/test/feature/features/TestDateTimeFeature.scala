package test.feature.features

import org.senseml.feature.features.DateTimeFeature
import org.senseml.feature.util.DateUtil

/**
  * TestDateTimeFeature
  *
  * Created by xueyintao on 2019-01-11.
  */
object TestDateTimeFeature {

  def testMake(): Unit = {
    val dt = DateUtil.parseDateTime("2018-12-11 18:50:01")

    val row = DateTimeFeature.make(dt)
    println(row)

  }

  def main(args: Array[String]): Unit = {

    testMake()
  }

}
