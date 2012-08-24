package com.comcast.xfinity.sirius.uberstore.seqindex

import com.comcast.xfinity.sirius.NiceTest
import java.io.RandomAccessFile
import collection.immutable.SortedMap
import org.mockito.Mockito._
import org.mockito.Matchers.{eq => meq, same, any}
import java.util.{TreeMap => JTreeMap}
import collection.JavaConversions._

object SeqIndexTest {

  def newMockFileOps(initialIndex: JTreeMap[Long, Long] = new JTreeMap[Long, Long]()): SeqIndexFileOps = {
    val mockOps = mock(classOf[SeqIndexFileOps])
    doReturn(initialIndex).when(mockOps).loadIndex(any[RandomAccessFile])
    mockOps
  }
}

class SeqIndexTest extends NiceTest {

  import SeqIndexTest._

  it ("must properly populate the instance from the passed in handle") {
    val mockHandle = mock[RandomAccessFile]

    val initialEntries = new JTreeMap[Long, Long](
      SortedMap(1L -> 2L, 3L -> 4L)
    )
    val mockFileOps = newMockFileOps(initialEntries)

    val underTest = new SeqIndex(mockHandle, mockFileOps)

    verify(mockFileOps).loadIndex(same(mockHandle))

    assert(Some(2L) === underTest.getOffsetFor(1L))
    assert(Some(4L) === underTest.getOffsetFor(3L))
    assert(None === underTest.getOffsetFor(10L))
  }

  it ("must apply updates to both the handle and memory") {
    val mockHandle = mock[RandomAccessFile]
    val mockFileOps = newMockFileOps()

    val underTest = new SeqIndex(mockHandle, mockFileOps)

    assert(None === underTest.getOffsetFor(1L))

    underTest.put(1L, 2L)
    assert(Some(2L) === underTest.getOffsetFor(1L))
    verify(mockFileOps).put(same(mockHandle), meq(1L), meq(2L))
  }

  describe ("getMaxSeq") {
    it ("must properly reflect maxSeq for an empty index") {
      val underTest = new SeqIndex(mock[RandomAccessFile], newMockFileOps())
      assert(None === underTest.getMaxSeq)
    }

    it ("must properly reflect maxSeq for a populated index") {
      val initialIndex = new JTreeMap[Long, Long](SortedMap(1L -> 2L))
      val underTest = new SeqIndex(mock[RandomAccessFile], newMockFileOps(initialIndex))

      assert(Some(1L) === underTest.getMaxSeq)

      underTest.put(3L, 4L)
      assert(Some(3L) === underTest.getMaxSeq)
    }
  }

  describe ("getOffsetRange") {
    val initialIndex: JTreeMap[Long, Long] = new JTreeMap(
      SortedMap(1L -> 2L, 3L -> 4L, 5L -> 6L)
    )
    val underTest = new SeqIndex(mock[RandomAccessFile], newMockFileOps(initialIndex))

    it ("must return (0, -1) if the range is empty") {
      assert((0L, -1L) === underTest.getOffsetRange(10, 20))
    }

    it ("must return the entire offset range inclusively") {
      assert((2L, 6L) === underTest.getOffsetRange(1, 5))
    }

    it ("must only include the specified range") {
      assert((4L, 6L) === underTest.getOffsetRange(3L, 5L))
    }

    it ("must return correct range if last sequence is missing in index") {
      assert((2L, 2L) === underTest.getOffsetRange(1L, 2L))
    }

    it ("must return correct range if first sequence is missing in index") {
      assert((4L, 4L) === underTest.getOffsetRange(2L, 3L))
    }
  }

  describe ("close") {
    it ("should close any provided filehandles") {
      val mockHandle = mock[RandomAccessFile]
      val underTest = new SeqIndex(mockHandle, newMockFileOps())

      underTest.close()

      verify(mockHandle).close()
    }
  }

}