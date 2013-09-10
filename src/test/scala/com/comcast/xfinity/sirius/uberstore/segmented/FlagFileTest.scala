package com.comcast.xfinity.sirius.uberstore.segmented

import com.comcast.xfinity.sirius.NiceTest
import java.io.File
import org.scalatest.BeforeAndAfterAll
import scalax.file.Path

class FlagFileTest extends NiceTest with BeforeAndAfterAll {
  val tempDir: File = {
    val tempDirName = "%s/flagfile-test-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    val dir = new File(tempDirName)
    dir.mkdirs()
    dir
  }

  override def afterAll() {
    Path.fromString(tempDir.getAbsolutePath).deleteRecursively(force = true)
  }

  describe("FlagFile") {
    it("should instantiate properly if the File exists") {
      val file = new File(tempDir, "flag-exists")
      file.createNewFile()

      assert(true === FlagFile(file.getAbsolutePath).value)
    }
    it("should instantiate properly if the File does not exist") {
      val file = new File(tempDir, "flag-does-not-exist")

      assert(false === FlagFile(file.getAbsolutePath).value)
    }
    it("should create the file if it is set to true") {
      val file = new File(tempDir, "not-yet-true")
      val flag = FlagFile(file.getAbsolutePath)

      flag.set(value = true)
      assert(true === file.exists())
    }
    it("should delete the file if it is set to false") {
      val file = new File(tempDir, "not-yet-true")
      val flag = FlagFile(file.getAbsolutePath)

      file.createNewFile()

      flag.set(value = false)
      assert(false === file.exists())
    }
  }

}
