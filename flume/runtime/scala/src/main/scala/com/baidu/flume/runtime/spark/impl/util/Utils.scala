/***************************************************************************
 *
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

package com.baidu.flume.runtime.spark.impl.util

import java.io.File

import com.baidu.flume.runtime.spark.Logging
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkEnv

/**
  * Created by Xianjin YE on 28/08/2017.
  */
object Utils extends Logging {


  /**
    * Default max number of sequence fields can be used to format truncatedString. This can be
    * overridden by setting the 'spark.debug.maxToStringFields' conf in SparkEnv
    *
    * This field and related truncatedString method comes froms Spark's Utils.scala as it's private
    * to Spark.
    */
  val DEFAULT_MAX_TO_STRING_FIELDS = 10

  private def maxNumToStringFields = {
    if (SparkEnv.get != null) {
      SparkEnv.get.conf.getInt("spark.debug.maxToStringFields", DEFAULT_MAX_TO_STRING_FIELDS)
    } else {
      DEFAULT_MAX_TO_STRING_FIELDS
    }
  }


  /**
    * Format a sequence with semantics similar to calling .mkString(). Any elements beyond
    * maxLength will be dropped and replaced by a "... N more fields" placeholder.
    *
    * @return the trimmed and formatted string.
    */
  def truncatedString[T](seq: Seq[T],
                         start: String,
                         sep: String,
                         end: String,
                         maxNumFields: Int = maxNumToStringFields): String = {
    if (seq.length > maxNumFields) {
      val numFields = math.max(0, maxNumFields - 1)
      seq.take(numFields).mkString(
        start, sep, sep + "... " + (seq.length - numFields) + " more fields" + end)
    } else {
      seq.mkString(start, sep, end)
    }
  }

  /** Shorthand for calling truncatedString() without start or end strings. */
  def truncatedString[T](seq: Seq[T], sep: String): String = truncatedString(seq, "", sep, "")


  /**
    * Detect common items in dir1 and dir2
    * @param dir1 local directory assumed
    * @param dir2 local directory assumed
    * @return List[File], common items in dir1 and dire2
    */
  def dirCommonItems(dir1: File, dir2: File): List[File] = {
    if (dir1.isDirectory && dir2.isDirectory) {
      val subFiles1 = dir1.listFiles()
      val subFiles2 = dir2.listFiles()
      subFiles1.intersect(subFiles2).toList
    } else {
      List()
    }
  }

  /**
    * Merge a sequence of comma-separated file lists, some of which may be null/empty to indicate
    * no files, into a single comma-separated string.
    * @param lists variable string argument
    * @return String, merged file lists. null if empty.
    */
  def mergeCacheFileLists(lists: String*): String = {
    val merged = lists.filterNot(StringUtils.isBlank)
                      .flatMap(_.split(","))
                      .mkString(",")
    if (merged == "") null else merged
  }

}
