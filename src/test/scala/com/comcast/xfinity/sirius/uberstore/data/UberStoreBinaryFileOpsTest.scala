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

package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.NiceTest
import common.Checksummer
import data.UberStoreBinaryFileOps
import java.io.RandomAccessFile
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.Matchers.{any, anyInt}
import java.util.Arrays
import java.nio.{ByteOrder, ByteBuffer}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

object UberStoreBinaryFileOpsTest {
  trait FauxChecksummer extends Checksummer {
    var nextChecksum: Long = 0L

    def setNextChecksum(chksum: Long) {
      nextChecksum = chksum
    }

    def checksum(bytes: Array[Byte]): Long = nextChecksum
  }

  def mockReadAnswerForBytes(bytes: Array[Byte], toReturn: Int) =
    new Answer[Int] {
      def answer(invocation: InvocationOnMock) = {
        val args = invocation.getArguments
        System.arraycopy(bytes, 0, args(0), 0, bytes.length)
        toReturn
      }
    }
}

class UberStoreBinaryFileOpsTest extends NiceTest {

  import UberStoreBinaryFileOpsTest._

  describe("put") {
    it ("must properly encode the event and write it to the handle, returning its offset") {
      val underTest = new UberStoreBinaryFileOps with FauxChecksummer

      val bytes = "hello world".getBytes
      val dummyOffset = 5678L
      val dummyChecksum = 9876L

      val mockHandle = mock[RandomAccessFile]
      doReturn(dummyOffset).when(mockHandle).getFilePointer
      underTest.setNextChecksum(dummyChecksum)

      val captor = ArgumentCaptor.forClass(classOf[Array[Byte]])

      // check we get the proper offset out
      assert(dummyOffset === underTest.put(mockHandle, bytes))

      verify(mockHandle).write(captor.capture())

      val expectedBytes = ByteBuffer.allocate(4 + 8 + bytes.length).
                                              order(ByteOrder.BIG_ENDIAN).
                                              putInt(bytes.length).
                                              putLong(dummyChecksum).
                                              put(bytes).array

      assert(Arrays.equals(expectedBytes, captor.getValue),
        "Did not get expected bytes")
    }
  }

  describe("readNext") {
    it ("must return None if at the end of the file") {
      val underTest = new UberStoreBinaryFileOps with FauxChecksummer

      val mockHandle = mock[RandomAccessFile]

      val dummyFileLen = 1234L
      doReturn(dummyFileLen).when(mockHandle).length
      doReturn(dummyFileLen).when(mockHandle).getFilePointer

      assert(None === underTest.readNext(mockHandle))
    }

    it ("must return Some(bytes) if the next entry is available and the checksum checks out") {
      val underTest = new UberStoreBinaryFileOps with FauxChecksummer

      val mockHandle = mock[RandomAccessFile]

      val expectedBody = "it puts the lotion on its skin".getBytes
      val dummyChecksum = 123456789L

      underTest.setNextChecksum(dummyChecksum)
      val theHeader = ByteBuffer.allocate(4 + 8).putInt(expectedBody.length).putLong(dummyChecksum).array

      doReturn(0L).when(mockHandle).getFilePointer
      doReturn(10000L).when(mockHandle).length

      val readHeaderAnswer = mockReadAnswerForBytes(theHeader, theHeader.length)
      val readBodyAnswer = mockReadAnswerForBytes(expectedBody, expectedBody.length)

      // XXX: readFully is final, but it just calls through to read/3, so we can mock that
      doAnswer(readHeaderAnswer).doAnswer(readBodyAnswer).when(mockHandle).read(any[Array[Byte]], anyInt, anyInt)

      underTest.readNext(mockHandle) match {
        case None => assert(false, "Expected some data, but there was none")
        case Some(readBody) =>
          assert(Arrays.equals(expectedBody, readBody),
            "Body was not as expected")
      }
    }

    it ("must throw an IllegalStateException if the checksum doesn't check out") {
      val underTest = new UberStoreBinaryFileOps with FauxChecksummer

      val mockHandle = mock[RandomAccessFile]

      val expectedBody = "or it gets the hose again".getBytes
      val dummyChecksum = 123456789L

      underTest.setNextChecksum(dummyChecksum + 1)
      val theHeader = ByteBuffer.allocate(4 + 8).putInt(expectedBody.length).putLong(dummyChecksum).array

      doReturn(0L).when(mockHandle).getFilePointer
      doReturn(10000L).when(mockHandle).length

      val readHeaderAnswer = mockReadAnswerForBytes(theHeader, theHeader.length)
      val readBodyAnswer = mockReadAnswerForBytes(expectedBody, expectedBody.length)

      // XXX: readFully is final, but it just calls through to read/3, so we can mock that
      doAnswer(readHeaderAnswer).doAnswer(readBodyAnswer).when(mockHandle).read(any[Array[Byte]], anyInt, anyInt)

      intercept[IllegalStateException] {
        underTest.readNext(mockHandle)
      }
    }
  }
}
