/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import java.io.{File => JFile}

import better.files.File
import better.files.Dsl._

object FileBasedClusterConfigTest {
  def createTempDir: JFile = {
    Thread.sleep(5)
    val tempDirName = "%s/file-based-cluster-config-test-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    mkdirs(File(tempDirName))
    new JFile(tempDirName)
  }
}

class FileBasedClusterConfigTest extends NiceTest {
  var tempDir: JFile = _
  val lineSep: String = System.lineSeparator()

  def writeClusterConfig(dir: JFile, lines: List[String]): File = {
    val clusterConfig = File(new JFile(dir, "sirius.cluster.config").getAbsolutePath)
    clusterConfig.delete(swallowIOExceptions = true)
    clusterConfig.write(lines.mkString(lineSep))
    clusterConfig
  }

  before {
    tempDir = FileBasedClusterConfigTest.createTempDir
  }
  after {
    File(tempDir.getPath).delete(swallowIOExceptions = true)
  }

  describe("FileBasedClusterConfig") {
    it("should return all lines of a normal file") {
      val file = writeClusterConfig(tempDir, List("one", "two", "three"))
      val underTest = FileBasedClusterConfig(file.pathAsString)

      assert("onetwothree" === underTest.members.reduce(_ + _))
    }
    it("should ignore commented lines") {
      val file = writeClusterConfig(tempDir, List("one", "# two", "three"))
      val underTest = FileBasedClusterConfig(file.pathAsString)

      assert("onethree" === underTest.members.reduce(_ + _))
    }
    it("should respond to changes in the underlying file") {
      val file = writeClusterConfig(tempDir, List("one", "two", "three"))
      val underTest = FileBasedClusterConfig(file.pathAsString)

      assert("onetwothree" === underTest.members.reduce(_ + _))

      writeClusterConfig(tempDir, List("one", "#two", "three"))
      assert("onethree" === underTest.members.reduce(_ + _))
    }
  }
}
