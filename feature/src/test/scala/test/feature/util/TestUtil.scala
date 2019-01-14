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
package test.feature.util

import java.text.SimpleDateFormat

import org.senseml.feature.util.DateUtil

/**
  * TestUtil
  *
  * Author: YintaoXue (ruogu.org)
  * Date: 2019-01-14
  */
object TestUtil {

  def main(args: Array[String]): Unit = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    // test DateUtil
    val date1 = new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-31")
    val date2 = new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-28")
    val diffDays = DateUtil.diffDays(date1, date2)
    println(diffDays)

    val adddate = DateUtil.addDate(date1, -1)
    val adddate2 = DateUtil.addDate(date1, 1)
    println("addDate(-1): " + sdf.format(adddate))
    println("addDate(1): " + sdf.format(adddate2))

  }

}
