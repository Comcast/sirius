package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.impl.persistence.LogRange
import scalax.io.CloseableIterator
import com.comcast.xfinity.sirius.api.impl.{Put, Delete, OrderedEvent}

object UberToolTest {
  class DummySiriusLog(var events: List[OrderedEvent]) extends SiriusLog {
    def writeEntry(event: OrderedEvent) {
      // terribly inefficient, I know, but this is for a test so who cares
      events = events :+ event
    }

    def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T =
      events.foldLeft(acc0)(foldFun)

    def foldLeftRange[T](start: Long, end: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T = {
      val startIndex = events.indexWhere((event) => event.sequence == start)
      val endIndex = events.indexWhere((event) => event.sequence == end)
      events.slice(startIndex, endIndex).foldLeft(acc0)(foldFun)
    }

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

  describe("compact") {
    it ("must compact the input into the output") {
      val uncompactedEvents = List(
        OrderedEvent(1, 2, Put("A", "check".getBytes)),
        OrderedEvent(2, 5, Delete("A")),
        OrderedEvent(3, 8, Put("B", "yourself".getBytes)),
        OrderedEvent(4, 2, Delete("B")),
        OrderedEvent(5, 5, Put("C", "before".getBytes)),
        OrderedEvent(6, 2, Delete("C")),
        OrderedEvent(7, 5, Put("D", "you".getBytes)),
        OrderedEvent(8, 5, Delete("D")),
        OrderedEvent(9, 5, Put("E", "wreck".getBytes)),
        OrderedEvent(10, 5, Delete("E")),
        OrderedEvent(11, 5, Put("F", "yourself".getBytes)),
        OrderedEvent(12, 5, Delete("F"))
      )

      val inLog = new DummySiriusLog(uncompactedEvents)
      val outLog = new DummySiriusLog(Nil)

      UberTool.compact(inLog, outLog)

      val outEvents = outLog.foldLeft(List[OrderedEvent]())(
        (acc, evt) => evt :: acc
      ).reverse
      val expected = List(
        OrderedEvent(2, 5, Delete("A")),
        OrderedEvent(4, 2, Delete("B")),
        OrderedEvent(6, 2, Delete("C")),
        OrderedEvent(8, 5, Delete("D")),
        OrderedEvent(10, 5, Delete("E")),
        OrderedEvent(12, 5, Delete("F"))
      )
      assert(expected === outEvents)
    }
  }

  describe("twoPassCompact") {
    it ("must compact the input into the output") {
      val uncompactedEvents = List(
        OrderedEvent(1, 2, Put("A", "check".getBytes)),
        OrderedEvent(2, 5, Delete("A")),
        OrderedEvent(3, 8, Put("B", "yourself".getBytes)),
        OrderedEvent(4, 2, Delete("B")),
        OrderedEvent(5, 5, Put("C", "before".getBytes)),
        OrderedEvent(6, 2, Delete("C")),
        OrderedEvent(7, 5, Put("D", "you".getBytes)),
        OrderedEvent(8, 5, Delete("D")),
        OrderedEvent(9, 5, Put("E", "wreck".getBytes)),
        OrderedEvent(10, 5, Delete("E")),
        OrderedEvent(11, 5, Put("F", "yourself".getBytes)),
        OrderedEvent(12, 5, Delete("F"))
      )

      val inLog = new DummySiriusLog(uncompactedEvents)
      val outLog = new DummySiriusLog(Nil)

      UberTool.twoPassCompact(inLog, outLog)

      val outEvents = outLog.foldLeft(List[OrderedEvent]())(
        (acc, evt) => evt :: acc
      ).reverse
      val expected = List(
        OrderedEvent(2, 5, Delete("A")),
        OrderedEvent(4, 2, Delete("B")),
        OrderedEvent(6, 2, Delete("C")),
        OrderedEvent(8, 5, Delete("D")),
        OrderedEvent(10, 5, Delete("E")),
        OrderedEvent(12, 5, Delete("F"))
      )
      assert(expected === outEvents)
    }
  }
}