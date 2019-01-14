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
package org.senseml.feature.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * DateUtil
  *
  * Created by xueyintao on 2019-01-11.
  */
object DateUtil {

  val DATE_TIME_FMT = "yyyy-MM-dd HH:mm:ss"
  val DATE_FMT = "yyyy-MM-dd"

  def parseDateTime(date: String): Date = {
    val sdf = new SimpleDateFormat(DATE_TIME_FMT)
    sdf.parse(date)
  }

  def parseDate(date: String): Date = {
    val sdf = new SimpleDateFormat(DATE_FMT)
    sdf.parse(date)
  }

  def parseDate(date: String, fmt: String): Date = {
    val sdf = new SimpleDateFormat(fmt)
    sdf.parse(date)
  }

  def diffDays(date1: Date, date2: Date): Int = {
    val diffms = date1.getTime - date2.getTime
    val nd = 1000 * 60 * 60 * 24
    (diffms / nd).toInt
  }

  def addDate(date: Date, days: Int): Date = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, days)
    calendar.getTime
  }

}
