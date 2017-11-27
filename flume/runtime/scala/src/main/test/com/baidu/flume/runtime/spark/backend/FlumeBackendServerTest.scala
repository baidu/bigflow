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

package com.baidu.flume.runtime.spark.backend

import org.scalatest.FunSuite

/**
  * Created by Xianjin YE on 20/09/2017.
  */
class FlumeBackendServerTest extends FunSuite {


  test("testInit") {
    val server = new FlumeBackendServer
    server.init()
    assert(1024 to 65535 contains(server.thriftServer.listenPost))
  }

  test("testExit") {
    // this can not be tested
  }

  test("testStopOfferService") {
    // nothing here
  }

  test("testWaitForExit") {
    val server = new FlumeBackendServer
    val stopThriftServerThread = new Thread("Stop Thrift Server") {
      setDaemon(true)

      override def run(): Unit = {
        Thread.sleep(100) // sleep 100ms
        server.stopOfferService()
      }
    }
    server.init()
    stopThriftServerThread.start()
    server.waitForExit() // This blocks here
    // This means we are done here
    assert(true)
  }
}
