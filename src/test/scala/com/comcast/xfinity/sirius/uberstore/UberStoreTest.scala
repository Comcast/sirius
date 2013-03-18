package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.NiceTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import org.mockito.Mockito._

@RunWith(classOf[JUnitRunner])
class UberStoreTest extends NiceTest {

  var underTest: UberStore = _
  val mockPair = mock[UberStoreFilePair]
  val mockOrderedEvent = mock[OrderedEvent]

  before {
    underTest = new UberStore(mockPair)
  }

  describe("all methods should delegate to UberStoreFilePair") {
    it ("including writeEntry") {
      underTest.writeEntry(mockOrderedEvent)
      verify(mockPair).writeEntry(mockOrderedEvent)
    }

    it ("including getNextSeq") {
      underTest.getNextSeq
      verify(mockPair).getNextSeq
    }

    it ("including foldLeft") {
      val acc = 0L
      val foldFun = ((l: Long, o: OrderedEvent) => 0L)
      underTest.foldLeft(acc)(foldFun)
      verify(mockPair).foldLeft(acc)(foldFun)
    }

    it ("including foldLeftRange") {
      val acc = 0L
      val (start, end) = (0L, 50L)
      val foldFun = ((l: Long, o: OrderedEvent) => 0L)
      underTest.foldLeftRange(start, end)(acc)(foldFun)
      verify(mockPair).foldLeftRange(start, end)(acc)(foldFun)
    }

    it ("including close") {
      underTest.close()
      verify(mockPair).close()
    }
    it ("including isClosed") {
      underTest.isClosed
      verify(mockPair).isClosed
    }
  }
}
