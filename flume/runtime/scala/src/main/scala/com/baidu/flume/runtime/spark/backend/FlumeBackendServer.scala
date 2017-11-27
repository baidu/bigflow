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

import java.io.{DataOutputStream, File, FileOutputStream}
import java.net.{InetAddress, ServerSocket}

import baidu.flume.thrift_gen.TBackendService
import com.baidu.flume.runtime.spark.{Logging, SparkContextWrapper}
import org.apache.thrift.TProcessorFactory


/**
  * The main entry point for Flume's Spark Driver server. Starts up a `BackendThriftServer`.
  *
  * @author Ye, Xianjin(yexianjin@baidu.com)
  */
class FlumeBackendServer extends Logging {


  private[backend] var thriftServer: BackendThriftServer = _

  private def initRPCServer(): Int = {
    val backendServiceImpl = new FlumeBackendServiceImpl(this)
    val processorFactory = new TProcessorFactory(
      new TBackendService.AsyncProcessor[FlumeBackendServiceImpl](backendServiceImpl)
    )
    thriftServer =
      new BackendThriftServer(BackendThriftServer.Args(processorFactory=processorFactory))
    thriftServer.start()
    thriftServer.listenPost
  }

  private def stopRPCServer(): Unit = {
    this.thriftServer.stop()
  }

  def init(): Int = {
    log.info("Flume Backend starts")
    initRPCServer()
  }

  def waitForExit(): Unit = {
    log.warn("Flume Backend wait for exit")
    try {
      thriftServer.join()
    } catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
  }

  def stopOfferService(): Unit = {
    stopRPCServer()
  }

  def exit(retCode: Int = 0): Unit = {
    log.info("Flume Backend exit")
    this.stopOfferService()

    this.stopRPCServer()

    System.exit(retCode)
  }

}

object FlumeBackendServer extends Logging {

  type OptionMap = Map[Symbol, String]

  def showUsageAndExit(extra: String = ""): Unit = {
    val usage =
      """
      Usage: FlumeBackendServer <--prepared_archive_path path> <--application_archive path> <--temp_setup_file path>
      """.stripMargin

    if (!extra.isEmpty) {
      Console.err.println(extra)
    }
    Console.err.println(usage)
    sys.exit(-1)
  }

  def parseArgs(argList: List[String]): OptionMap = {

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--pb_job_message" :: value :: tail =>
          nextOption(map ++ Map('pb_job_message -> value.toString), tail)
        case "--prepared_archive_path" :: value :: tail =>
          nextOption(map ++ Map('prepared_archive_path -> value.toString), tail)
        case "--application_archive" :: value :: tail =>
          nextOption(map ++ Map('application_archive -> value.toString), tail)
        case "--tmp_setup_file" ::value:: tail =>
          nextOption(map ++ Map('tmp_setup_file -> value.toString), tail)
        case option :: _ =>
          showUsageAndExit(s"Unknown option: $option")
          null
      }
    }

    val parsed = nextOption(Map(), argList)

    if (!(parsed.contains('prepared_archive_path) && parsed.contains('application_archive)
      && parsed.contains('tmp_setup_file))) {
      showUsageAndExit()
    }
    parsed
  }

  def main(args: Array[String]): Unit = {

    val argList = args.toList

    val options = parseArgs(argList)

    SparkContextWrapper.preparedArchivePath = options('prepared_archive_path)
    SparkContextWrapper.applicationArchive = options('application_archive)

    val server = new FlumeBackendServer()
    val listenPort = server.init()
    val monitorSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"))
    val monitorPort = monitorSocket.getLocalPort()

    log.info(s"listen port: $listenPort, monitor port: $monitorPort")
    val f = new File(options('tmp_setup_file) + ".tmp")
    f.deleteOnExit() // if any thing bad happens
    val dos = new DataOutputStream(new FileOutputStream(f))
    dos.writeInt(listenPort)
    dos.writeInt(monitorPort)
    dos.close()
    f.renameTo(new File(options('tmp_setup_file)))

    // wait for the end of backend's socket, then exit
    new Thread("wait for socket to close") {
      setDaemon(true)
      override def run(): Unit = {
        val buf = new Array[Byte](1024)
        // shutdown JVM if backend does not connect back in 10 seconds
        monitorSocket.setSoTimeout(10000)
        try {
          val inSocket = monitorSocket.accept()
          monitorSocket.close()
          inSocket.getInputStream().read(buf)
        } finally {
          server.exit(64) // randomly pick a return code in 64-113
        }
      }
    }.start()

    server.waitForExit()

    server.exit()
  }
}