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
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.impl.{Put, Delete, OrderedEvent}
import java.io.File
import scalax.file.Path
import com.comcast.xfinity.sirius.uberstore.segmented.SegmentedUberStore

object UberToolTest {
  class DummySiriusLog(var events: List[OrderedEvent]) extends SiriusLog {
    def writeEntry(event: OrderedEvent) {
      // terribly inefficient, I know, but this is for a test so who cares
      events = events :+ event
    }

    def foldLeftRange[T](start: Long, end: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T =
      events.filter(e => start <= e.sequence && e.sequence <= end).foldLeft(acc0)(foldFun)

    def getNextSeq: Long =
      throw new IllegalStateException("not implemented")

    def compact {}

    def size = 0L
  }

  def createTempDir = {
    val tempDirName = "%s/ubertool-test-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    val file = new File(tempDirName)
    file.mkdirs()
    file
  }
}

class UberToolTest extends NiceTest {
  import UberToolTest._

  describe("isLegacy") {
    it ("must return true for a legacy uberstore") {
      val location = createTempDir.getAbsolutePath
      val uberstore = UberStore(location)
      uberstore.writeEntry(OrderedEvent(1L, 1L, Delete("1")))

      assert(true === UberTool.isLegacy(location))

      Path.fromString(location).deleteRecursively(force = true)
    }
    it ("must return false for a segmented uberstore") {
      val location = createTempDir.getAbsolutePath
      SegmentedUberStore.init(location)
      val uberstore = SegmentedUberStore(location)
      uberstore.writeEntry(OrderedEvent(1L, 1L, Delete("1")))

      assert(false === UberTool.isLegacy(location))

      Path.fromString(location).deleteRecursively(force = true)
    }
  }

  describe("isSegmented") {
    it ("must return true for a segmented uberstore") {
      val location = createTempDir.getAbsolutePath
      SegmentedUberStore.init(location)
      val uberstore = SegmentedUberStore(location)
      uberstore.writeEntry(OrderedEvent(1L, 1L, Delete("1")))

      assert(true === UberTool.isSegmented(location))

      Path.fromString(location).deleteRecursively(force = true)
    }
    it ("must return false for a legacy uberstore") {
      val location = createTempDir.getAbsolutePath
      val uberstore = UberStore(location)
      uberstore.writeEntry(OrderedEvent(1L, 1L, Delete("1")))

      assert(false === UberTool.isSegmented(location))

      Path.fromString(location).deleteRecursively(force = true)
    }
  }

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

    it ("must be ok with compacting an empty log") {
      val inLog = new DummySiriusLog(Nil)
      val outLog = new DummySiriusLog(Nil)

      UberTool.compact(inLog, outLog)

      // a size method on SiriusLog would be cool...
      assert(0 === outLog.foldLeft(0)((a, _) => a + 1))
    }

    it ("must remove deletes before cutoff timestamp") {
      val uncompactedEvents = List(
        OrderedEvent(1, 100, Put("TooOld", "will be fully crushed".getBytes)),
        OrderedEvent(2, 200, Delete("TooOld")),
        OrderedEvent(3, 300, Put("Slayer", "Rules".getBytes)),
        OrderedEvent(4, 400, Put("TooNew", "delete will remain".getBytes)),
        OrderedEvent(5, 500, Delete("TooNew"))
      )

      val inLog = new DummySiriusLog(uncompactedEvents)
      val outLog = new DummySiriusLog(Nil)

      UberTool.compact(inLog, outLog, 400)

      val outEvents = outLog.foldLeft(List[OrderedEvent]())(
        (acc, evt) => evt :: acc
      ).reverse
      val expected = List(
        OrderedEvent(3, 300, Put("Slayer", "Rules".getBytes)),
        OrderedEvent(5, 500, Delete("TooNew"))
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

    it ("must be ok with compacting an empty log") {
      val inLog = new DummySiriusLog(Nil)
      val outLog = new DummySiriusLog(Nil)

      UberTool.twoPassCompact(inLog, outLog)

      // a size method on SiriusLog would be cool...
      assert(0 === outLog.foldLeft(0)((a, _) => a + 1))
    }

        it ("must remove deletes before cutoff timestamp") {
      val uncompactedEvents = List(
        OrderedEvent(1, 100, Put("TooOld", "will be fully crushed".getBytes)),
        OrderedEvent(2, 200, Delete("TooOld")),
        OrderedEvent(3, 300, Put("Slayer", "Rules".getBytes)),
        OrderedEvent(4, 400, Put("TooNew", "delete will remain".getBytes)),
        OrderedEvent(5, 500, Delete("TooNew"))
      )

      val inLog = new DummySiriusLog(uncompactedEvents)
      val outLog = new DummySiriusLog(Nil)

      UberTool.twoPassCompact(inLog, outLog, 400)

      val outEvents = outLog.foldLeft(List[OrderedEvent]())(
        (acc, evt) => evt :: acc
      ).reverse
      val expected = List(
        OrderedEvent(3, 300, Put("Slayer", "Rules".getBytes)),
        OrderedEvent(5, 500, Delete("TooNew"))
      )
      assert(expected === outEvents)
    }
  }
}
