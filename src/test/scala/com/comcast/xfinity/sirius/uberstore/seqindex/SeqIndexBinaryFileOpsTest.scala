package com.comcast.xfinity.sirius.uberstore.seqindex

import com.comcast.xfinity.sirius.NiceTest
import java.io.RandomAccessFile
import com.comcast.xfinity.sirius.uberstore.Checksummer
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

object SeqIndexBinaryFileOpsTest {
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

class SeqIndexBinaryFileOpsTest extends NiceTest {

  import SeqIndexBinaryFileOpsTest._

  describe("put") {
    it ("must properly record the Seq -> Offset mapping with checksum") {
      val underTest = new SeqIndexBinaryFileOps with FauxChecksummer

      val theSeq: Long = 10
      val theOffset: Long = 20

      val dummyChecksum = 12345L

      val mockHandle = mock[RandomAccessFile]
      underTest.setNextChecksum(dummyChecksum)

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
      val underTest = new SeqIndexBinaryFileOps with FauxChecksummer

      val mockHandle = mock[RandomAccessFile]

      val dummyChecksum: Long = 1234
      underTest.setNextChecksum(dummyChecksum)

      val dummyFileLen: Long = 48 // two entries
      doReturn(dummyFileLen).when(mockHandle).length

      // read two entries
      doReturn(0L).doReturn(24L).doReturn(48L).when(mockHandle).getFilePointer

      val entry1Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum).putLong(1L).putLong(2L).array
      val entry1Answer = mockReadAnswerForBytes(entry1Bytes, entry1Bytes.length)

      val entry2Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum).putLong(2L).putLong(3L).array
      val entry2Answer = mockReadAnswerForBytes(entry2Bytes, entry2Bytes.length)

      // XXX: readFully is final, but it just calls through to read/3, so we can mock that
      doAnswer(entry1Answer).doAnswer(entry2Answer).when(mockHandle).read(any[Array[Byte]], anyInt, anyInt)

      val actual = underTest.loadIndex(mockHandle)
      val expected = new JTreeMap[Long, Long](SortedMap(1L -> 2L, 2L -> 3L))

      assert(actual === expected)
    }

    it ("must throw an IllegalStateException if corruption is detected") {
      val underTest = new SeqIndexBinaryFileOps with FauxChecksummer

      val mockHandle = mock[RandomAccessFile]

      val dummyChecksum: Long = 1234
      underTest.setNextChecksum(dummyChecksum)

      val dummyFileLen: Long = 48 // two entries
      doReturn(dummyFileLen).when(mockHandle).length

      // read two entries
      doReturn(0L).doReturn(24L).doReturn(48L).when(mockHandle).getFilePointer

      val entry1Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum).putLong(1L).putLong(2L).array
      val entry1Answer = mockReadAnswerForBytes(entry1Bytes, entry1Bytes.length)

      // This is the corrupted one!
      val entry2Bytes = ByteBuffer.allocate(24).putLong(dummyChecksum + 1).putLong(2L).putLong(3L).array
      val entry2Answer = mockReadAnswerForBytes(entry2Bytes, entry2Bytes.length)

      // XXX: readFully is final, but it just calls through to read/3, so we can mock that
      doAnswer(entry1Answer).doAnswer(entry2Answer).when(mockHandle).read(any[Array[Byte]], anyInt, anyInt)

      intercept[IllegalStateException] {
        underTest.loadIndex(mockHandle)
      }
    }
  }
}