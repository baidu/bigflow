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

import java.io.IOException
import java.net.ServerSocket

import scala.util.Random

import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.{TThreadPoolServer, TThreadedSelectorServer}
import org.apache.thrift.transport.{TNonblockingServerSocket, TServerSocket}

import com.baidu.flume.runtime.spark.Logging
import com.baidu.flume.runtime.spark.backend.BackendThriftServer.Args

/**
  * A Thrift Server Wrapper, implementation came from BBSThriftServer
  *
  * @author Ye, Xianjin(yexianjin@baidu.com)
  */
class BackendThriftServer(val args: Args) extends Logging {
  private val protocolFactory = new TBinaryProtocol.Factory(true, true)
  private val serverPort: Int = if (args.serverPort > 0) {
    args.serverPort
  } else {
    BackendThriftServer.selectFreePort(args.portRange)
  }

  log.warn(s"Thrift Server init, serverPort: $serverPort")

  // init thrift server
  private val server = if (args.nonBlocking) {
    val serverTransport = new TNonblockingServerSocket(serverPort)
    val serverArgs = new TThreadedSelectorServer.Args(serverTransport)
      .selectorThreads(args.selectorThreads)
      .workerThreads(args.minWorkerThreads)
      .protocolFactory(protocolFactory)
      .processorFactory(args.processorFactory)

    serverArgs.maxReadBufferBytes = args.maxReadBufferBytes
    new TThreadedSelectorServer(serverArgs)
  } else {
    val serverTransport = new TServerSocket(serverPort)
    val serverArgs = new TThreadPoolServer.Args(serverTransport)
      .processorFactory(args.processorFactory)
      .protocolFactory(protocolFactory)
      .minWorkerThreads(args.minWorkerThreads)
      .maxWorkerThreads(args.maxWorkThreads)

    new TThreadPoolServer(serverArgs)
  }

  def start(): Unit = {
    new Thread(new Runnable {
        override def run(): Unit = {
          log.warn(s"Start a ThriftServer, server : $server")
          server.serve()
        }
    }).start()
  }

  def join(): Unit = {
    // todo: deal with suspected
    do {
      Thread.sleep(1000);
    } while (server.isServing)
    log.warn(s"ThriftServer exit, server: $server")
  }

  def stop(): Unit = {
    server.stop()
  }

  def listenPost: Int = serverPort
}

object BackendThriftServer {

  /**
    * The default minimum value for unspecified port range
    */
  val PORT_RANGE_MIN = 1024

  /**
    * The default maximum value for unspecified port range
    */
  val PORT_RANGE_MAX = 65535

  private val random = new Random(System.currentTimeMillis())

  /**
    * Configuration controls how the thrift server is constructed.
    * @param nonBlocking specify thrift server running under nonBlocking or blocking manner
    * @param selectorThreads num of selector threads
    * @param minWorkerThreads the minimum num of worker threads
    * @param maxWorkThreads the maximum num of work threads
    * @param serverPort the port num thrift server should listen on. <=0 indicates server can choose
    *                   freely in the [[portRange]]
    * @param portRange (min, max) indicates port range thrift server can choose, only effective when
    *                  [[serverPort <= 0]]. If not specified(passed as null),
    *                  ([[PORT_RANGE_MIN]], [[PORT_RANGE_MAX]]) is assumed.
    * @param maxReadBufferBytes the maximum bytes in thrift server's read buffer
    * @param processorFactory the processor factory that create processor handles incoming RPC
    *                         requests
    */
  case class Args(nonBlocking: Boolean = true,
                  selectorThreads: Int = 2,
                  minWorkerThreads: Int = 4,
                  maxWorkThreads: Int = 8,
                  serverPort: Int = 0,
                  portRange: (Int, Int) = (10000, 50000),
                  maxReadBufferBytes: Long = 2 * 1024 * 1024,
                  processorFactory: TProcessorFactory)

  /**
    * Select a free port in portRange using randomized port selection.
    *
    * PortRange is required to be 0 < portRange._1 < portRange._2 <= PORT_RANGE_MAX(65535) if
    * presented, otherwise default range (PORT_RANGE_MIN(1024), PORT_RANGE_MAX(65535)) is used
    *
    * Note:
    *      1. The selected free port can be unavailable before binding, this is considered rare and
    *         BackendThriftServer wouldn't cover that
    *      2. Total number of port selection is portRange._2 - portRange._1. If no port can be
    *         selected after total number of attempts, An IllegalStateException will be thrown
    *
    * @param portRange a tuple[Int, Int], could be null
    * @return a free port number
    */
  private def selectFreePort(portRange: (Int, Int)): Int = {
    val (minPort, maxPort) = if (portRange != null) portRange else (PORT_RANGE_MIN, PORT_RANGE_MAX)
    require(minPort > 0, "minPort in PortRange must be greater than 0")
    require(maxPort > minPort, "maxPort in PortRange must be greater than minPort")
    require(maxPort <= PORT_RANGE_MAX,
      s"maxPort in PortRange must be less than or equal to $PORT_RANGE_MAX")
    val numPorts = maxPort - minPort

    for (_ <- 0 until numPorts) {
      val candidatePort = minPort + random.nextInt(numPorts + 1)
      try {
        val socket = new ServerSocket(candidatePort)
        socket.close()
        return candidatePort
      } catch {
        case _: IOException =>
      }
    }

    throw new IllegalStateException(s"Could not find an available port in range " +
      s"[$minPort, $maxPort] after $numPorts attempts")
  }
}