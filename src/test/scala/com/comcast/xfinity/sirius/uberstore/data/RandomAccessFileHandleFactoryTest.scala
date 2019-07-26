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

class RandomAccessFileHandleFactoryTest extends UberDataFileHandleTest {
  describe("createWriteHandle") {
    it("creates a random access write handle to the file") {
      writeRandomBytes(256)

      val underTest = RandomAccessFileHandleFactory
      val result = underTest.createWriteHandle(tempPath.toString)

      try {
        assert(result.isInstanceOf[RandomAccessFileWriteHandle])
        assert(result.offset() == 256L)
      } finally {
        result.close()
      }
    }
  }
  describe("createReadHandle") {
    it("creates a random access read handle to the file") {
      writeRandomBytes(256)

      val underTest = RandomAccessFileHandleFactory
      val result = underTest.createReadHandle(tempPath.toString, 0L)

      try {
        assert(result.isInstanceOf[RandomAccessFileReadHandle])
        assert(result.offset() == 0L)
      } finally {
        result.close()
      }
    }
    it("creates a random access read handle to the file at the specified offset") {
      writeRandomBytes(256)

      val underTest = RandomAccessFileHandleFactory
      val result = underTest.createReadHandle(tempPath.toString, 100L)

      try {
        assert(result.isInstanceOf[RandomAccessFileReadHandle])
        assert(result.offset() == 100L)
      } finally {
        result.close()
      }
    }
  }
}
