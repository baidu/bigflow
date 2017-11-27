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

import baidu.flume.thrift_gen.TBackendService
import org.apache.hadoop.hive.ql.index.TableBasedIndexHandler
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TSocket, TTransport}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by Xianjin YE on 20/09/2017.
  */
class FlumeBackendServiceImplTest extends FunSuite with BeforeAndAfterAll {
  private var backend: FlumeBackendServer = _
  override def beforeAll() {
    super.beforeAll()
    backend = new FlumeBackendServer
    backend.init()
  }

  override def afterAll() {
    backend.stopOfferService()
  }

  private def withBackendServiceClient(f: TBackendService.Client => Unit): Unit = {
    val rawTransport = new TSocket("localhost", backend.thriftServer.listenPost)
    val transport = new TFramedTransport(rawTransport)
    val protocol = new TBinaryProtocol(transport)
    val client = new TBackendService.Client(protocol)

    transport.open()
    try f(client) finally transport.close()
  }

  test("testRunJob") {
    withBackendServiceClient { client =>
      // todo: how should we run physical job. Maybe we should mock flume backend service.
    }
  }

  test("testGetCachedData") {
    // todo: mock flume backend service
  }

  test("testStop") {
    // can not be tested
  }

}
