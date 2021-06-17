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

import java.io.{EOFException, RandomAccessFile}

import org.mockito.Mockito._

import scala.util.Random

class RandomAccessFileReadHandleTest extends UberDataFileHandleTest {

  describe("eof") {
    it("returns false when getFilePointer is less than length") {
      val mockRandomAccessFile = mock[RandomAccessFile]
      doReturn(10L).when(mockRandomAccessFile).getFilePointer
      doReturn(20L).when(mockRandomAccessFile).length()

      val underTest = new RandomAccessFileReadHandle(mockRandomAccessFile)
      val result = underTest.eof()
      assert(!result)
    }
    it("returns true when at end of file") {
      val mockRandomAccessFile = mock[RandomAccessFile]
      doReturn(20L).when(mockRandomAccessFile).getFilePointer
      doReturn(20L).when(mockRandomAccessFile).length()

      val underTest = new RandomAccessFileReadHandle(mockRandomAccessFile)
      val result = underTest.eof()
      assert(result)
    }
  }
  describe("readInt") {
    it("returns 32-bit Integer and advances offset") {
      val expected = Random.nextInt()
      writeInts(expected)

      val underTest = RandomAccessFileReadHandle(tempPath.toString, 0L)
      val result = underTest.readInt()
      assert(result == expected)
      assert(underTest.offset() == 4)
    }
    it("reads 32-bit Integer at offset and advances offset") {
      val expected = Random.nextInt()
      val baseOffset = Random.nextInt(500)
      writeRandomBytes(baseOffset)
      writeInts(expected)

      val underTest = RandomAccessFileReadHandle(tempPath.toString, baseOffset)
      val result = underTest.readInt()
      assert(result == expected)
      assert(underTest.offset() == baseOffset + 4)
    }
    it("throws EOFException when EOF") {
      val underTest = RandomAccessFileReadHandle(tempPath.toString, 0L)
      intercept[EOFException] {
        val _ = underTest.readInt()
      }
    }
  }
  describe("readLong") {
    it("returns 64-bit Integer and advances offset") {
      val expected = Random.nextLong()
      writeLongs(expected)
      val underTest = RandomAccessFileReadHandle(tempPath.toString, 0L)
      val result = underTest.readLong()
      assert(result == expected)
      assert(underTest.offset() == 8)
    }
    it("returns 64-bit Integer at offset and advances offset") {
      val baseOffset = Random.nextInt(500)
      val expected = Random.nextLong()
      writeRandomBytes(baseOffset)
      writeLongs(expected)

      val underTest = RandomAccessFileReadHandle(tempPath.toString, baseOffset)
      val result = underTest.readLong()
      assert(result == expected)
      assert(underTest.offset() == baseOffset + 8)
    }
    it("throws EOFException when EOF") {
      val underTest = RandomAccessFileReadHandle(tempPath.toString, 0L)
      intercept[EOFException] {
        val _ = underTest.readLong()
      }
    }
  }
  describe("readFully") {
    it("fills array and advances offset") {
      val expected = randomBytes(10)
      writeBytes(expected: _*)
      val underTest = RandomAccessFileReadHandle(tempPath.toString, 0L)
      val array = new Array[Byte](10)
      underTest.readFully(array)
      assert(array.sameElements(expected))
      assert(underTest.offset() == 10)
    }
    it("fills array at offset and advances offset") {
      val baseOffset = Random.nextInt(500)
      val expected = randomBytes(10)
      writeRandomBytes(baseOffset)
      writeBytes(expected: _*)
      val underTest = RandomAccessFileReadHandle(tempPath.toString, baseOffset)
      val array = new Array[Byte](10)
      underTest.readFully(array)
      assert(array.sameElements(expected))
      assert(underTest.offset() == baseOffset + 10)
    }
    it("throws EOFException when EOF") {
      val underTest = RandomAccessFileReadHandle(tempPath.toString, 0L)
      intercept[EOFException] {
        val array = new Array[Byte](10)
        underTest.readFully(array)
      }
    }
    it("throws EOFException when less than required bytes remaining") {
      writeRandomBytes(7)
      val underTest = RandomAccessFileReadHandle(tempPath.toString, 0L)
      intercept[EOFException] {
        val array = new Array[Byte](10)
        underTest.readFully(array)
      }
    }
  }
  describe("close") {
    it("closes the underlying RandomAccessFile") {
      val mockRandomAccessFile = mock[RandomAccessFile]
      val underTest = new RandomAccessFileReadHandle(mockRandomAccessFile)
      underTest.close()

      verify(mockRandomAccessFile).close()
    }
  }
}
