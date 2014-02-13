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
import java.io.File
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine

object FileBasedClusterConfigTest {
  def createTempDir = {
    Thread.sleep(5)
    val tempDirName = "%s/file-based-cluster-config-test-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    new File(tempDirName)
  }
}

class FileBasedClusterConfigTest extends NiceTest {
  var tempDir: File = _

  def writeClusterConfig(dir: File, lines: List[String]): Path = {
    val clusterConfig = Path.fromString(new File(dir, "sirius.cluster.config").getAbsolutePath)
    clusterConfig.delete()
    clusterConfig.writeStrings(lines, NewLine.sep)
    clusterConfig
  }

  before {
    tempDir = FileBasedClusterConfigTest.createTempDir
  }
  after {
    Path(tempDir).deleteRecursively()
  }

  describe("FileBasedClusterConfig") {
    it("should return all lines of a normal file") {
      val file = writeClusterConfig(tempDir, List("one", "two", "three"))
      val underTest = FileBasedClusterConfig(file.path)

      assert("onetwothree" === underTest.members.reduce(_ + _))
    }
    it("should ignore commented lines") {
      val file = writeClusterConfig(tempDir, List("one", "# two", "three"))
      val underTest = FileBasedClusterConfig(file.path)

      assert("onethree" === underTest.members.reduce(_ + _))
    }
    it("should respond to changes in the underlying file") {
      val file = writeClusterConfig(tempDir, List("one", "two", "three"))
      val underTest = FileBasedClusterConfig(file.path)

      assert("onetwothree" === underTest.members.reduce(_ + _))

      writeClusterConfig(tempDir, List("one", "#two", "three"))
      assert("onethree" === underTest.members.reduce(_ + _))
    }
  }
}
