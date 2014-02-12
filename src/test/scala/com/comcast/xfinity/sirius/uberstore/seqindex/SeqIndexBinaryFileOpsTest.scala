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

package com.comcast.xfinity.sirius.uberstore.seqindex

import com.comcast.xfinity.sirius.NiceTest
import java.io.RandomAccessFile
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import java.nio.{ByteOrder, ByteBuffer}
import java.util.Arrays
import org.mockito.Matchers.{any, anyInt}
import collection.immutable.SortedMap
import java.util.{TreeMap => JTreeMap}
import collection.JavaConversions._
import com.comcast.xfinity.sirius.uberstore.common.Checksummer

object SeqIndexBinaryFileOpsTest {

  def mockReadAnswerForBytes(bytes: Array[Byte], toReturn: Int) =
    new Answer[Int] {
      def answer(invocation: InvocationOnMock) = {
        val args = invocation.getArguments
        System.arraycopy(bytes, 0, args(0), 0, bytes.length)
        toReturn
      }
    }
}

class SeqIndexBinaryFileOpsTest extends NiceTest {

  import SeqIndexBinaryFileOpsTest._

  describe("put") {
    it ("must properly record the Seq -> Offset mapping with checksum") {
      val mockChecksummer = mock[Checksummer]
      val underTest = new SeqIndexBinaryFileOps(mockChecksummer)

      val theSeq: Long = 10
      val theOffset: Long = 20

      val dummyChecksum = 12345L

      val mockHandle = mock[RandomAccessFile]
      doReturn(dummyChecksum).when(mockChecksummer).checksum(any[Array[Byte]])

      val captor = ArgumentCaptor.forClass(classOf[Array[Byte]])

      underTest.put(mockHandle, theSeq, theOffset)

      verify(mockHandle).write(captor.capture())

      val expectedBytes = ByteBuffer.allocate(24).
                                      order(ByteOrder.BIG_ENDIAN).
                                      putLong(dummyChecksum).
                                      putLong(theSeq).
                                      putLong(theOffset).array

      assert(Arrays.equals(expectedBytes, captor.getValue),
        "Did not get expected bytes")
    }
  }

  describe("loadIndex") {
    it ("must read to the end of the file and return all the mappings it finds") {
      val mockChecksummer = mock[Checksummer]
      val underTest = new SeqIndexBinaryFileOps(mockChecksummer)

      val mockHandle = mock[RandomAccessFile]

      val dummyChecksum: Long = 1234
      doReturn(dummyChecksum).when(mockChecksummer).checksum(any[Array[Byte]])

      val entry1Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum).putLong(1L).putLong(2L).array
      val entry2Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum).putLong(2L).putLong(3L).array

      val chunkBytes = ByteBuffer.allocate(48).put(entry1Bytes).put(entry2Bytes).array
      val chunkAnswer = mockReadAnswerForBytes(chunkBytes, chunkBytes.length)

      doAnswer(chunkAnswer).when(mockHandle).read(any[Array[Byte]])

      val actual = underTest.loadIndex(mockHandle)
      val expected = new JTreeMap[Long, Long](SortedMap(1L -> 2L, 2L -> 3L))

      assert(actual === expected)
    }

    it ("must throw an IllegalStateException if corruption is detected") {
      val mockChecksummer = mock[Checksummer]
      val underTest = new SeqIndexBinaryFileOps(mockChecksummer)

      val mockHandle = mock[RandomAccessFile]

      val dummyChecksum: Long = 1234
      doReturn(dummyChecksum).when(mockChecksummer).checksum(any[Array[Byte]])

      val entry1Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum).putLong(1L).putLong(2L).array
      // This is the corrupted one!
      val entry2Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum + 1).putLong(2L).putLong(3L).array

      val chunkBytes = ByteBuffer.allocate(48).put(entry1Bytes).put(entry2Bytes).array
      val chunkAnswer = mockReadAnswerForBytes(chunkBytes, chunkBytes.length)

      // XXX: readFully is final, but it just calls through to read/3, so we can mock that
      doAnswer(chunkAnswer).when(mockHandle).read(any[Array[Byte]])

      intercept[IllegalStateException] {
        underTest.loadIndex(mockHandle)
      }
    }

    it ("must properly read over chunk boundaries") {
      val mockChecksummer = mock[Checksummer]
      val underTest = new SeqIndexBinaryFileOps(mockChecksummer, 48)

      val mockHandle = mock[RandomAccessFile]

      val dummyChecksum: Long = 1234
      doReturn(dummyChecksum).when(mockChecksummer).checksum(any[Array[Byte]])

      val entry1Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum).putLong(1L).putLong(2L).array
      val entry2Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum).putLong(2L).putLong(3L).array
      val chunk1Bytes = ByteBuffer.allocate(48).put(entry1Bytes).put(entry2Bytes).array
      val chunk1Answer = mockReadAnswerForBytes(chunk1Bytes, chunk1Bytes.length)

      val entry3Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum).putLong(3L).putLong(4L).array
      val chunk2Bytes = ByteBuffer.allocate(24).put(entry3Bytes).array
      val chunk2Answer = mockReadAnswerForBytes(chunk2Bytes, chunk2Bytes.length)

      doAnswer(chunk1Answer).doAnswer(chunk2Answer).when(mockHandle).read(any[Array[Byte]])

      val actual = underTest.loadIndex(mockHandle)
      val expected = new JTreeMap[Long, Long](SortedMap(1L -> 2L, 2L -> 3L, 3L -> 4L))

      assert(actual === expected)
    }
  }
}
