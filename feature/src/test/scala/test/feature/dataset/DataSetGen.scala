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
package test.feature.dataset

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * generate test dataset with random
  */
object DataSetGen {

  def main(args: Array[String]): Unit = {

    val rows = 1000

    var data = new ArrayBuffer[String]()
    for (i <- 0 to rows) {
      // order_id, user_id, city, industry, price, cnt, create_time
      val order_id = i + 1
      val user_id = Random.nextInt(300)
      val city = Random.nextInt(10)
      val industry = Random.nextInt(20)
      val price = Random.nextDouble() * 2000
      val priceStr = price.formatted("%.2f")
      val quantity = Random.nextInt(5)
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, Random.nextInt(100) * -1)
      cal.set(Calendar.HOUR_OF_DAY, Random.nextInt(24))
      cal.set(Calendar.MINUTE, Random.nextInt(60))
      cal.set(Calendar.SECOND, Random.nextInt(60))
      val create_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cal.getTime)

      val rs = "" + order_id + "," + user_id + "," + city + "," + industry + "," + priceStr + "," +
        quantity + "," + create_time

      data += rs
    }

    val out = new FileWriter("dataset/orders.txt", false)

    for (line <- data) {
      println(line)
      out.write(line + "\n")
    }
    out.flush()
    out.close()

  }

}
