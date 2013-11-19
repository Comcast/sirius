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
