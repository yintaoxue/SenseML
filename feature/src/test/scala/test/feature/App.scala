package test.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.senseml.feature.Features
import org.senseml.feature.util.EnvUtil

import scala.collection.mutable

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

    val orders_raw = spark.read.text(ordersPath)
    val ordersDF = orders_raw.map {
      row =>
        val f = row.getString(0).split(",")
        (f(0).toInt, f(1).toInt, f(2).toInt, f(3).toInt, f(4).toDouble, f(5).toInt, f(6))
    }.toDF("order_id", "user_id", "city", "industry", "price", "quantity", "create_time")

    ordersDF.cache()
    ordersDF.show()
    val schema = ordersDF.schema
    println(schema)

    // make data time feature from create_time
    val rs = Features.makeDateTimeFeature(spark, ordersDF, "create_time")
    rs.show()
    println(rs.schema)

    // agg features
    val aggMap = new mutable.HashMap[String, String]()
    aggMap.put("price", "sum")
    aggMap.put("price", "avg")
    aggMap.put("price", "max")
    aggMap.put("price", "min")
    aggMap.put("quantity", "sum")

    val rs2 = Features.makeAggFeature(spark, ordersDF, "user_id", "price,quantity")
    rs2.show()

  }

}
