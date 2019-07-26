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

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Arrays

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.uberstore.common.Checksummer
import com.comcast.xfinity.sirius.uberstore.data.{UberDataFileReadHandle, UberDataFileWriteHandle, UberStoreBinaryFileOps}
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

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

      val mockHandle = mock[UberDataFileWriteHandle]
      doReturn(dummyOffset).when(mockHandle).write(any[Array[Byte]])
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

      val mockHandle = mock[UberDataFileReadHandle]

      doReturn(true).when(mockHandle).eof()

      assert(None === underTest.readNext(mockHandle))
    }

    it ("must return Some(bytes) if the next entry is available and the checksum checks out") {
      val underTest = new UberStoreBinaryFileOps with FauxChecksummer

      val mockHandle = mock[UberDataFileReadHandle]

      val expectedBody = "it puts the lotion on its skin".getBytes
      val dummyChecksum = 123456789L

      underTest.setNextChecksum(dummyChecksum)

      doReturn(false).when(mockHandle).eof()
      doReturn(expectedBody.length).when(mockHandle).readInt()
      doReturn(dummyChecksum).when(mockHandle).readLong()

      val readBodyAnswer = mockReadAnswerForBytes(expectedBody, expectedBody.length)

      doAnswer(readBodyAnswer).when(mockHandle).readFully(any[Array[Byte]])

      underTest.readNext(mockHandle) match {
        case None => assert(false, "Expected some data, but there was none")
        case Some(readBody) =>
          assert(Arrays.equals(expectedBody, readBody),
            "Body was not as expected")
      }
    }

    it ("must throw an IllegalStateException if the checksum doesn't check out") {
      val underTest = new UberStoreBinaryFileOps with FauxChecksummer

      val mockHandle = mock[UberDataFileReadHandle]

      val expectedBody = "or it gets the hose again".getBytes
      val dummyChecksum = 123456789L

      underTest.setNextChecksum(dummyChecksum + 1)
      val theHeader = ByteBuffer.allocate(4 + 8).putInt(expectedBody.length).putLong(dummyChecksum).array

      doReturn(false).when(mockHandle).eof()
      doReturn(expectedBody.length).when(mockHandle).readInt()
      doReturn(dummyChecksum).when(mockHandle).readLong()

      val readBodyAnswer = mockReadAnswerForBytes(expectedBody, expectedBody.length)

      doAnswer(readBodyAnswer).when(mockHandle).readFully(any[Array[Byte]])

      intercept[IllegalStateException] {
        underTest.readNext(mockHandle)
      }
    }
  }
}
