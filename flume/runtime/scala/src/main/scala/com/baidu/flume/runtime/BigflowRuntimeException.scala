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

package com.baidu.flume.runtime

/**
  * The Exception thrown at Bigflow runtime
  *
  * @author Wang, Cong(bigflow-opensource@baidu.com)
  */
class BigflowRuntimeException(msg: String, cause: Throwable)
  extends Exception(BigflowRuntimeException.defaultMessage(msg, cause), cause) {

  def this(msg: String) = this(msg, null)
  def this() = this(null)
}

object BigflowRuntimeException {
  def defaultMessage(msg: String, cause: Throwable) =
    if (msg != null) msg
    else if (cause != null) cause.toString
    else null
}

