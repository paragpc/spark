/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.api.python

import java.io.File
import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

class VirtualEnvFactory(pythonExec: String, conf: SparkConf, isDriver: Boolean)
  extends Logging {

  private val virtualEnvType = conf.get("spark.pyspark.virtualenv.type", "native")
  private val virtualEnvBinPath = conf.get("spark.pyspark.virtualenv.bin.path", "")
  private val initPythonPackages = conf.getOption("spark.pyspark.virtualenv.packages")
  private var virtualEnvName: String = _
  private var virtualPythonExec: String = _
  private val VIRTUALENV_ID = new AtomicInteger()
  private var isLauncher: Boolean = false

  // used by launcher when user want to use virtualenv in pyspark shell.
  def this(pythonExec: String, properties: JMap[String, String], isDriver: java.lang.Boolean) {
    this(pythonExec, new SparkConf().setAll(properties.asScala), isDriver)
    this.isLauncher = true
  }

  def setupVirtualEnv(): String = {
    /*
     * Native Virtualenv:
     *   -  Execute command: virtualenv -p <pythonExec> --system-site-packages <virtualenvName>
     */
    logInfo("Start to setup virtualenv...")
    logDebug("user.dir=" + System.getProperty("user.dir"))
    logDebug("user.home=" + System.getProperty("user.home"))

    require(virtualEnvType == "native",
      s"VirtualEnvType: $virtualEnvType is not supported." )
    require(new File(virtualEnvBinPath).exists(),
      s"VirtualEnvBinPath: $virtualEnvBinPath is not defined or doesn't exist.")
    // Two scenarios of creating virtualenv:
    // 1. created in yarn container. Yarn will clean it up after container is exited
    // 2. created outside yarn container. Spark need to create temp directory and clean it after app
    //    finish.
    //      - driver of PySpark shell
    //      - driver of yarn-client mode
    if (isLauncher ||
      (isDriver && conf.get("spark.submit.deployMode") == "client")) {
      val virtualenvBasedir = Files.createTempDir()
      virtualenvBasedir.deleteOnExit()
      virtualEnvName = virtualenvBasedir.getAbsolutePath
    } else {
      // isDriver && conf.get("spark.submit.deployMode") == "cluster")
      // OR Executor
      // Use the working directory
      virtualEnvName = "virtualenv_" + conf.getAppId + "_" + VIRTUALENV_ID.getAndIncrement()
    }

    val createEnvCommand = List(virtualEnvBinPath, "-p", pythonExec,
      "--system-site-packages", virtualEnvName)
    execCommand(createEnvCommand)

    virtualPythonExec = virtualEnvName + "/bin/python"
    // install packages
    if (initPythonPackages.isDefined) {
      val packages = initPythonPackages.get.trim
      if (!packages.isEmpty) {
        execCommand(List(virtualPythonExec, "-m", "pip",
          "install") ::: packages.replace(" ", "").split(":").toList);
      }
    }
    logInfo(s"virtualenv is created at $virtualPythonExec")
    virtualPythonExec
  }

  private def execCommand(commands: List[String]): Unit = {
    logInfo("Running command:" + commands.mkString(" "))
    val pb = new ProcessBuilder(commands.asJava)
    // don't inheritIO when it is used in launcher, because launcher would capture the standard
    // output to assemble the spark-submit command.
    if(!isLauncher) {
      pb.inheritIO();
    }
    pb.environment().put("HOME", System.getProperty("user.home"))
    val proc = pb.start()
    val exitCode = proc.waitFor()
    if (exitCode != 0) {
      throw new RuntimeException("Fail to run command: " + commands.mkString(" "))
    }
  }
}
