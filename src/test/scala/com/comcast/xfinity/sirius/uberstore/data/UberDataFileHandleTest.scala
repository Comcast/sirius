/*
 *  Copyright 2012-2019 Comcast Cable Communications Management, LLC
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

package com.comcast.xfinity.sirius.uberstore.data

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.file.{Files, Path, StandardOpenOption}

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfter

import scala.util.Random

trait UberDataFileHandleTest extends NiceTest with BeforeAndAfter {

  var tempPath: Path = _

  before {
    tempPath = Files.createTempFile(getClass.getSimpleName, "test")
  }
  after {
    Files.delete(tempPath)
  }

  def write(f: OutputStream => Unit): Unit = {
    val outputStream = Files.newOutputStream(tempPath, StandardOpenOption.APPEND)
    try {
      f(outputStream)
    } finally {
      outputStream.close()
    }
  }

  def randomBytes(len: Int): Seq[Byte] = {
    val array = new Array[Byte](len)
    Random.nextBytes(array)
    array.toSeq
  }

  def writeRandomBytes(len: Int): Unit = writeBytes(randomBytes(len):_*)

  def writeBytes(bytes: Byte*): Unit = write { outputStream =>
    outputStream.write(bytes.toArray)
  }

  def writeInts(ints: Int*): Unit = write { outputStream =>
    val buffer = ByteBuffer.allocate(ints.length * 4)
    buffer.asIntBuffer().put(ints.toArray)
    outputStream.write(buffer.array())
  }

  def writeLongs(longs: Long*): Unit = write { outputStream =>
    val buffer = ByteBuffer.allocate(longs.length * 8)
    buffer.asLongBuffer().put(longs.toArray)
    outputStream.write(buffer.array())
  }

  def readBytes(): Array[Byte] = Files.readAllBytes(tempPath)
}
