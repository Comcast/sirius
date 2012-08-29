package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.impl.persistence.LogRange
import scalax.io.CloseableIterator
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}

object UberToolTest {
  class DummySiriusLog(var events: List[OrderedEvent]) extends SiriusLog {
    def writeEntry(event: OrderedEvent) {
      // terribly inefficient, I know, but this is for a test so who cares
      events = events :+ event
    }

    def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T =
      events.foldLeft(acc0)(foldFun)

    def getNextSeq: Long =
      throw new IllegalStateException("not implemented")

    def createIterator(range: LogRange): CloseableIterator[OrderedEvent] =
      throw new IllegalStateException("not implemented")
  }
}

class UberToolTest extends NiceTest {
  import UberToolTest._

  describe("copy") {
    it ("must copy the contents of the input to the output") {
      val events = List(
        OrderedEvent(1, 2, Delete("3")),
        OrderedEvent(4, 5, Delete("6")),
        OrderedEvent(7, 8, Delete("9"))
      )
      val inLog = new DummySiriusLog(events)
      val outLog = new DummySiriusLog(Nil)

      UberTool.copyLog(inLog, outLog)

      val outEvents = outLog.foldLeft(List[OrderedEvent]())(
        (acc, evt) => evt :: acc
      ).reverse
      assert(events === outEvents)
    }
  }
}