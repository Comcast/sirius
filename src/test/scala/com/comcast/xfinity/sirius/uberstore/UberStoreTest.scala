package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.NiceTest
import data.UberDataFile
import seqindex.SeqIndex
import org.mockito.Mockito._
import org.mockito.Matchers.{any, eq => meq, anyLong, same}
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}
import com.comcast.xfinity.sirius.api.impl.persistence.BoundedLogRange

object UberStoreTest {

  def createMockedUpLog: (UberDataFile, SeqIndex, UberStore) = {
    val mockDataFile = mock(classOf[UberDataFile])
    val mockIndex = mock(classOf[SeqIndex])
    val underTest = new UberStore(mockDataFile, mockIndex)
    (mockDataFile, mockIndex, underTest)
  }
}

class UberStoreTest extends NiceTest {

  import UberStoreTest._

  describe("writeEntry") {
    it ("must persist the event to the dataFile, and offset to the index") {
      val (mockDataFile, mockIndex, underTest) = createMockedUpLog

      doReturn(None).when(mockIndex).getMaxSeq
      doReturn(1234L).when(mockDataFile).writeEvent(any[OrderedEvent])

      val theEvent = OrderedEvent(5, 678, Delete("yarr"))
      underTest.writeEntry(theEvent)

      verify(mockDataFile).writeEvent(meq(theEvent))
      verify(mockIndex).put(meq(5L), meq(1234L))
    }

    it ("must not allow out of order events") {
      val (_, mockIndex, underTest) = createMockedUpLog

      doReturn(Some(5L)).when(mockIndex).getMaxSeq

      intercept[IllegalArgumentException] {
        underTest.writeEntry(OrderedEvent(1, 1234, Delete("FAIL")))
      }
    }
  }

  describe("getNextSeq") {
    it ("must return 1 if the index is empty") {
      val (_, mockIndex, underTest) = createMockedUpLog

      doReturn(None).when(mockIndex).getMaxSeq

      assert(1L === underTest.getNextSeq)
    }

    it ("must return 1 greater than the max if the index is populated") {
      val (_, mockIndex, underTest) = createMockedUpLog

      doReturn(Some(6L)).when(mockIndex).getMaxSeq

      assert(7L === underTest.getNextSeq)
    }
  }

  describe("foldLeft") {
    it ("must fold over the entire data file, as known by the index") {
      val (mockDataFile, mockIndex, underTest) = createMockedUpLog

      val theFoldFun = (s: Symbol, e: OrderedEvent) => s

      doReturn((0L, 1000L)).when(mockIndex).getOffsetRange(anyLong, anyLong)
      doReturn('orange).when(mockDataFile).foldLeftRange(anyLong, anyLong)(any[Symbol])(any[(Symbol, Long, OrderedEvent) => Symbol]())

      assert('orange === underTest.foldLeft('first)(theFoldFun))

      // Not really sure if there's a good way to verify this, the code is simple enough and this is tested
      //  by the integration test
      verify(mockDataFile).foldLeftRange(meq(0L), meq(1000L))(meq('first))(any[(Symbol, Long, OrderedEvent) => Symbol]())
    }
  }

  describe("createIterator") {
    it ("isn't implemented, dummy") {
      val (_, _, underTest) = createMockedUpLog

      intercept[IllegalStateException] {
        underTest.createIterator(BoundedLogRange(0, 100))
      }
    }
  }

  describe("isClosed") {
    val (mockDataFile, mockIndex, underTest) = createMockedUpLog
    it ("should return true if only index is closed") {
      doReturn(false).when(mockDataFile).isClosed
      doReturn(true).when(mockIndex).isClosed
      assert(true == underTest.isClosed)
    }
    it ("should return true if only datafile is closed") {
      doReturn(true).when(mockDataFile).isClosed
      doReturn(false).when(mockIndex).isClosed
      assert(true == underTest.isClosed)
    }
    it ("should return true if both index and datafile are closed") {
      doReturn(true).when(mockDataFile).isClosed
      doReturn(true).when(mockIndex).isClosed
      assert(true == underTest.isClosed)
    }
    it ("should return false if neither index nor datafile are closed") {
      doReturn(false).when(mockDataFile).isClosed
      doReturn(false).when(mockIndex).isClosed
      assert(false == underTest.isClosed)
    }
  }

  describe("close") {
    it ("should close underlying index and data") {
      val (mockDataFile, mockIndex, underTest) = createMockedUpLog
      doReturn(false).when(mockDataFile).isClosed
      doReturn(false).when(mockIndex).isClosed

      underTest.close()

      verify(mockDataFile).close()
      verify(mockIndex).close()
    }
  }


}