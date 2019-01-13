package org.senseml.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.senseml.feature.util.EnvUtil

import scala.collection.mutable

/**
  * App
  *
  * Created by xueyintao on 2019-01-13.
  */
object App {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN) // 避免打印大量日志

    run()

  }

  def run(): Unit = {
    val projectPath = EnvUtil.getProjectPath()
    println(projectPath)

    val ordersPath = projectPath + "dataset/orders.txt"

    val spark = SparkSession.builder().master("local").appName("Feature").getOrCreate()
    import spark.implicits._

    // load data to DF
    val orders_raw = spark.read.text(ordersPath)
    val ordersDF = orders_raw.map {
      row =>
        val f = row.getString(0).split(",")
        (f(0).toInt, f(1).toInt, f(2).toInt, f(3).toInt, f(4).toDouble, f(5).toInt, f(6))
    }.toDF("order_id", "user_id", "city", "industry", "price", "quantity", "create_time")

    ordersDF.cache()
    ordersDF.show()
    println(ordersDF.schema)

    // make data time feature from create_time
    val rs = Features.makeDateTimeFeature(spark, ordersDF, "create_time")
    rs.cache()
    rs.show()
    println(rs.schema)

    // group agg features
    val aggMap = new mutable.HashMap[String, String]()
    aggMap.put("price", "sum")
    aggMap.put("price", "avg")
    aggMap.put("price", "max")
    aggMap.put("price", "min")
    aggMap.put("quantity", "sum")

    val rs2 = Features.makeAggFeature(spark, ordersDF, List("user_id","city"), List("price","quantity"))
    rs2.show()

  }

}
