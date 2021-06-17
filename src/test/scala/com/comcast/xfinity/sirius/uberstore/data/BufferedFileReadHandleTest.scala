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

import java.io.{EOFException, InputStream}

import org.mockito.Mockito._

import scala.util.Random

class BufferedFileReadHandleTest extends UberDataFileHandleTest {

  val readBufferSize: Int = 8 * 1024

  describe("eof") {
    it("returns false when read returns a valid unsigned byte") {
      val inputStream = mock[InputStream]
      val expected = Random.nextInt(256)
      doReturn(expected).when(inputStream).read()

      val underTest = new BufferedFileReadHandle(inputStream, 0L)
      val result = underTest.eof()
      assert(!result)

      verify(inputStream).mark(1)
      verify(inputStream).reset()
    }
    it("returns true when read returns -1") {
      val inputStream = mock[InputStream]
      val expected = -1
      doReturn(expected).when(inputStream).read()

      val underTest = new BufferedFileReadHandle(inputStream, 0L)
      val result = underTest.eof()
      assert(result)

      verify(inputStream).mark(1)
      verify(inputStream).reset()
    }
  }
  describe("readInt") {
    it("returns 32-bit Integer and advances offset") {
      val expected = Random.nextInt()
      writeInts(expected)

      val underTest = BufferedFileReadHandle(tempPath.toString, 0L, readBufferSize)
      val result = underTest.readInt()
      assert(result == expected)
      assert(underTest.offset() == 4)
    }
    it("reads 32-bit Integer at offset and advances offset") {
      val expected = Random.nextInt()
      val baseOffset = Random.nextInt(500)
      writeRandomBytes(baseOffset)
      writeInts(expected)

      val underTest = BufferedFileReadHandle(tempPath.toString, baseOffset, readBufferSize)
      val result = underTest.readInt()
      assert(result == expected)
      assert(underTest.offset() == baseOffset + 4)
    }
    it("throws EOFException when EOF") {
      val underTest = BufferedFileReadHandle(tempPath.toString, 0L, readBufferSize)
      intercept[EOFException] {
        val _ = underTest.readInt()
      }
    }
  }
  describe("readLong") {
    it("returns 64-bit Integer and advances offset") {
      val expected = Random.nextLong()
      writeLongs(expected)
      val underTest = BufferedFileReadHandle(tempPath.toString, 0L, readBufferSize)
      val result = underTest.readLong()
      assert(result == expected)
      assert(underTest.offset() == 8)
    }
    it("returns 64-bit Integer at offset and advances offset") {
      val baseOffset = Random.nextInt(500)
      val expected = Random.nextLong()
      writeRandomBytes(baseOffset)
      writeLongs(expected)

      val underTest = BufferedFileReadHandle(tempPath.toString, baseOffset, readBufferSize)
      val result = underTest.readLong()
      assert(result == expected)
      assert(underTest.offset() == baseOffset + 8)
    }
    it("throws EOFException when EOF") {
      val underTest = BufferedFileReadHandle(tempPath.toString, 0L, readBufferSize)
      intercept[EOFException] {
        val _ = underTest.readLong()
      }
    }
  }
  describe("readFully") {
    it("fills array and advances offset") {
      val expected = randomBytes(10)
      writeBytes(expected: _*)
      val underTest = BufferedFileReadHandle(tempPath.toString, 0L, readBufferSize)
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
      val underTest = BufferedFileReadHandle(tempPath.toString, baseOffset, readBufferSize)
      val array = new Array[Byte](10)
      underTest.readFully(array)
      assert(array.sameElements(expected))
      assert(underTest.offset() == baseOffset + 10)
    }
    it("throws EOFException when EOF") {
      val underTest = BufferedFileReadHandle(tempPath.toString, 0L, readBufferSize)
      intercept[EOFException] {
        val array = new Array[Byte](10)
        underTest.readFully(array)
      }
    }
    it("throws EOFException when less than required bytes remaining") {
      writeRandomBytes(7)
      val underTest = BufferedFileReadHandle(tempPath.toString, 0L, readBufferSize)
      intercept[EOFException] {
        val array = new Array[Byte](10)
        underTest.readFully(array)
      }
    }
  }
  describe("close") {
    it("closes the underlying InputStream") {
      val mockInputStream = mock[InputStream]
      val underTest = new BufferedFileReadHandle(mockInputStream, 0L)
      underTest.close()

      verify(mockInputStream).close()
    }
  }
}
