package com.comcast.xfinity.sirius.uberstore.seqindex

import com.comcast.xfinity.sirius.NiceTest
import java.io.RandomAccessFile
import java.util.{TreeMap => JTreeMap}
import org.mockito.Matchers._
import org.mockito.Mockito._
import collection.SortedMap
import collection.JavaConversions._

object ReadOnlySeqIndexTest {

  def newMockFileOps(initialIndex: JTreeMap[Long, Long] = new JTreeMap[Long, Long]()): SeqIndexBinaryFileOps = {
    val mockOps = mock(classOf[SeqIndexBinaryFileOps])
    doReturn(initialIndex).when(mockOps).loadIndex(any[RandomAccessFile])
    mockOps
  }
}

class ReadOnlySeqIndexTest extends NiceTest {
  import ReadOnlySeqIndexTest._

  describe ("getMaxSeq") {
    it ("should update its backend index before replying") {
      val mockHandle = mock[RandomAccessFile]
      val mockFileOps = newMockFileOps()
      val underTest = new ReadOnlySeqIndex(mockHandle, mockFileOps)

      underTest.getMaxSeq
      // once on instantiation, once when getMaxSeq is called
      verify(mockFileOps, times(2)).loadIndex(mockHandle)
    }

    it ("should reply with None if the index is empty") {
      val mockHandle = mock[RandomAccessFile]
      val mockFileOps = newMockFileOps()

      val underTest = new ReadOnlySeqIndex(mockHandle, mockFileOps)
      assert(underTest.getMaxSeq === None)

    }

    it ("should reply correctly when backend has been changed between calls") {
      val mockHandle = mock[RandomAccessFile]

      val initialEntries = new JTreeMap[Long, Long](
        SortedMap(1L -> 2L, 3L -> 4L)
      )
      val mockFileOps = newMockFileOps(initialEntries)

      val underTest = new ReadOnlySeqIndex(mockHandle, mockFileOps)

      assert(underTest.getMaxSeq === Some(3L))

      val newEntries = new JTreeMap[Long, Long](
        SortedMap(1L -> 2L, 3L -> 4L, 5L -> 6L)
      )
      doReturn(newEntries).when(mockFileOps).loadIndex(mockHandle)

      assert(underTest.getMaxSeq === Some(5L))
    }
  }
  describe ("getOffsetFor") {
    it ("should update its backend index before replying") {
      val mockHandle = mock[RandomAccessFile]
      val mockFileOps = newMockFileOps()

      val underTest = new ReadOnlySeqIndex(mockHandle, mockFileOps)
      underTest.getOffsetFor(1L)

      // once on instantiation, once when getOffsetFor is called
      verify(mockFileOps, times(2)).loadIndex(mockHandle)
    }

    it ("should reply correctly when backend has been changed between calls") {
      val mockHandle = mock[RandomAccessFile]

      val initialEntries = new JTreeMap[Long, Long](
        SortedMap(1L -> 2L, 3L -> 4L)
      )
      val mockFileOps = newMockFileOps(initialEntries)
      val underTest = new ReadOnlySeqIndex(mockHandle, mockFileOps)

      assert(underTest.getOffsetFor(1L) === Some(2L))
      assert(underTest.getOffsetFor(5L) === None)

      val newEntries = new JTreeMap[Long, Long](
        SortedMap(1L -> 2L, 3L -> 4L, 5L -> 6L)
      )
      doReturn(newEntries).when(mockFileOps).loadIndex(mockHandle)

      assert(underTest.getOffsetFor(5L) === Some(6L))
    }
  }

  describe ("getOffsetRange") {
    it ("should update its backend index before replying") {
      val mockHandle = mock[RandomAccessFile]
      val mockFileOps = newMockFileOps()

      val underTest = new ReadOnlySeqIndex(mockHandle, mockFileOps)
      underTest.getOffsetRange(1L, 1L)

      // once on instantiation, once when getOffsetFor is called
      verify(mockFileOps, times(2)).loadIndex(mockHandle)
    }

    it ("should reply correctly when backend has been changed between calls") {
      val mockHandle = mock[RandomAccessFile]

      val initialEntries = new JTreeMap[Long, Long](
        SortedMap(1L -> 2L, 3L -> 4L)
      )
      val mockFileOps = newMockFileOps(initialEntries)
      val underTest = new ReadOnlySeqIndex(mockHandle, mockFileOps)

      assert(underTest.getOffsetRange(1L, 5L) === (2L, 4L))

      val newEntries = new JTreeMap[Long, Long](
        SortedMap(1L -> 2L, 3L -> 4L, 5L -> 6L)
      )
      doReturn(newEntries).when(mockFileOps).loadIndex(mockHandle)

      assert(underTest.getOffsetRange(1L, 5L) === (2L, 6L))
    }
  }

  describe ("put") {
    it ("should not be allowed") {
      val mockHandle = mock[RandomAccessFile]
      val mockFileOps = newMockFileOps()

      val underTest = new ReadOnlySeqIndex(mockHandle, mockFileOps)
      intercept[UnsupportedOperationException] {
        underTest.put(1L, 2L)
      }
    }
  }
}
