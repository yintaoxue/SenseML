package test.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.senseml.feature.Features
import org.senseml.feature.util.EnvUtil

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
    val orders = orders_raw.map {
      row =>
        val f = row.getString(0).split(",")
        (f(0).toInt, f(1).toInt, f(2).toInt, f(3).toInt, f(4).toDouble, f(5).toInt, f(6))
    }.toDF("order_id", "user_id", "city", "industry", "price", "quantity", "create_time")

    orders.cache()
    orders.show()
    val schema = orders.schema
    println(schema)

    // make data time feature from create_time
    val rs = Features.makeDateTimeFeature(spark, orders, "create_time")
    rs.show()
    println(rs.schema)
//    rs.select($"*", $"create_time").show()

  }

}
