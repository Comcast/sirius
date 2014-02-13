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
