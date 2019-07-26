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

import java.io.RandomAccessFile

import org.mockito.Mockito._

import scala.util.Random

class RandomAccessFileWriteHandleTest extends UberDataFileHandleTest {

  describe("apply") {
    it("seeks to the end of the file") {
      writeRandomBytes(50)
      val underTest = RandomAccessFileWriteHandle(tempPath.toString)
      try {
       assert(underTest.offset() == 50L)
      } finally {
        underTest.close()
      }
    }
  }
  describe("write") {
    it("writes to the underlying file") {
      val expected = new Array[Byte](50)
      Random.nextBytes(expected)

      val underTest = RandomAccessFileWriteHandle(tempPath.toString)
      try {
        underTest.write(expected)
      } finally {
        underTest.close()
      }

      val result = readBytes()
      assert(result sameElements expected)
    }
  }
  describe("offset") {
    it("returns the current filePointer") {
      val randomAccessFile = mock[RandomAccessFile]
      doReturn(1000L).when(randomAccessFile).getFilePointer

      val underTest = new RandomAccessFileWriteHandle(randomAccessFile)
      assert(underTest.offset() == 1000L)
    }
    it("returns the filePointer prior to writing the bytes") {
      val randomAccessFile = mock[RandomAccessFile]
      doReturn(1000L).when(randomAccessFile).getFilePointer

      val underTest = new RandomAccessFileWriteHandle(randomAccessFile)
      val bytes = new Array[Byte](50)
      Random.nextBytes(bytes)
      val result = underTest.write(bytes)

      assert(result == 1000L)
      val order = inOrder(randomAccessFile)
      order.verify(randomAccessFile).getFilePointer
      order.verify(randomAccessFile).write(bytes)
    }
  }
  describe("close") {
    it("calls close on the underlying RandomAccessFile") {
      val randomAccessFile = mock[RandomAccessFile]

      val underTest = new RandomAccessFileWriteHandle(randomAccessFile)
      underTest.close()
      verify(randomAccessFile).close()
    }
  }
}
