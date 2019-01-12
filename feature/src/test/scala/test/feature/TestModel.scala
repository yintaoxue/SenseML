package test.feature

import org.senseml.feature.model.Field

/**
  * TestModel
  *
  * Created by xueyintao on 2019-01-11.
  */
object TestModel {

  def main(args: Array[String]): Unit = {

    val YEAR = new Field("year", "y", Int)
    println(YEAR)

  }

}
