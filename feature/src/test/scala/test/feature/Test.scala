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
import scala.util.control.Breaks._
import org.senseml.feature.util.DateUtil

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Test
  *
  * Author: YintaoXue (ruogu.org)
  * Date: 2019-01-14
  */
object Test {

  def main(args: Array[String]): Unit = {

    testFuncDateWindow()

  }

  def testFuncDateWindow(): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val dt = sdf.parse("2018-12-31")
    val dtWindows: List[Int] = List(1,2,3,4)

    var windArray = ArrayBuffer[Array[Int]]()
    var cnt = 0
    for (t <- dtWindows) {
      val start = cnt
      val end = start + t - 1
      println(s"[$start, $end]")
      println(sdf.format(DateUtil.addDate(dt, start)) + "," + sdf.format(DateUtil.addDate(dt, end)))
      cnt += t

      windArray += Array(start, end)
    }


    val dt2 = DateUtil.addDate(dt, 2)
    println("dt2:" + sdf.format(dt2))

    val diffDays = DateUtil.diffDays(dt2, dt)

    var wind = -1
    breakable {
      for (i <- windArray) {
        wind += 1
        if (diffDays >= i(0) && diffDays <= i(1)) {
          break
        }
      }
    }

    println("wind:" + wind)


  }

}
