/*
 * Licensed to SenseML(http://senseml.org) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership. SenseML licenses this file to You
 * under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package test.feature

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.senseml.feature.Features
import org.senseml.feature.features.{StatisticFeature, TimeSeriesFeature}
import org.senseml.feature.util.EnvUtil
import scala.collection.mutable

/**
  * App
  *
  * Created by xueyintao on 2019-01-13.
  */
object App {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN) // avoid spark print too many logs

    run()

  }

  def run(): Unit = {
    val projectPath = EnvUtil.getProjectPath()
    println(projectPath)

    val ordersPath = projectPath + "dataset/orders.txt"

    val spark = SparkSession.builder().master("local").appName("Feature").getOrCreate()
    import spark.implicits._

    /** load data to DF */
    val orders_raw = spark.read.text(ordersPath)
    val ordersDF = orders_raw.map {
      row =>
        val f = row.getString(0).split(",")
        (f(0).toInt, f(1).toInt, f(2).toInt, f(3).toInt, f(4).toDouble, f(5).toInt, f(6))
    }.toDF("order_id", "user_id", "city", "industry", "price", "quantity", "create_time")

    ordersDF.cache()
    ordersDF.show()
    println(ordersDF.schema)

    /** data time feature */
//    val rs = Features.makeDateTimeFeature(spark, ordersDF, "create_time")
//    rs.cache()
//    rs.show()
//    println(rs.schema)
//
    /** group agg features */
    val aggMap = new mutable.HashMap[String, String]()
    aggMap.put("price", "sum")
    aggMap.put("price", "avg")
    aggMap.put("price", "max")
    aggMap.put("price", "min")
    aggMap.put("quantity", "sum")


    val rs1 = StatisticFeature.groupbyAggFeature(spark, ordersDF, List("user_id","order_id", "city"),
      List("price","quantity","industry"), StatisticFeature.defaultAggFuncs)
    println("\nStatisticFeature without join back:")
    rs1.show()

//    val rs2 = Features.makeAggFeature(spark, ordersDF, List("user_id","city"), List("price","quantity"))
//    rs2.show()
//
    /** time series features */
//    val tmpDF = ordersDF.withColumn("create_time", $"create_time".cast("date"))
//    tmpDF.show()
//
//    val sdf = new SimpleDateFormat("yyyy-MM-dd")
//    val startDT = sdf.parse("2018-12-31")
//    val dtWindows = List(1,2,3,4,5)

//    val rs3 = TimeSeriesFeature.makeTimeSeriesFeature(spark, tmpDF, "create_time", List("user_id"),
//      List("price", "quantity"), List("sum","count"), startDT, dtWindows)
//    rs3.orderBy($"user_id".asc, $"create_time__ts".asc)show(100)

//    val rs4 = TimeSeriesFeature.makeTimeSeriesFeatureColumns(spark, tmpDF, "create_time", List("user_id"),
//      List("price", "quantity"), List("sum","count"), startDT, dtWindows)
//    rs4.orderBy($"user_id".asc)show(100)


  }

}
