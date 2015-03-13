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

package com.comcast.xfinity.sirius.uberstore.segmented

import com.comcast.xfinity.sirius.NiceTest
import java.io.File
import com.comcast.xfinity.sirius.api.impl._
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.Delete
import scalax.file.Path
import com.comcast.xfinity.sirius.api.SiriusConfiguration

class SegmentedUberStoreTest extends NiceTest {

  def createTempDir = {
    val tempDirName = "%s/segmented-uberstore-test-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    SegmentedUberStore.init(tempDirName)
    new File(tempDirName)
  }

  def createSegment(baseDir: File, seq: String): File = {
    val dir = new File(baseDir, seq)
    dir.mkdir

    new File(dir, "index").createNewFile()
    new File(dir, "data").createNewFile()
    dir
  }

  def createPopulatedSegment(baseDir: File, name: String, events: List[Int], isApplied: Boolean = false) {
    val segment = Segment(baseDir, name)
    segment.setApplied(applied = isApplied)

    writeEvents(segment, events.map(_.toLong))
  }

  def findSegment(name: String, segments: List[Segment]) =
    segments.find(_.name == name).get

  def writeUberStoreEvents(target: SegmentedUberStore, keys: List[Int]) {
    for (key <- keys) {
      target.writeEntry(OrderedEvent(target.nextSeq, 0L, Delete(key.toString)))
    }
  }

  def writeEvents(segment: Segment, events: List[Long]) {
    for (i <- events) {
      segment.writeEntry(OrderedEvent(i, 0L, Delete(i.toString)))
    }
  }

  def makeSegment(fullPath: String): Segment = {
    val file = new File(fullPath)
    Segment(file.getParentFile, file.getName)
  }

  def listEvents(segment: Segment) =
    segment.foldLeft(List[String]())((acc, event) => event.request.key :: acc).reverse.mkString(" ")

  def listEvents(uberstore: SegmentedUberStore) =
    uberstore.foldLeft(List[String]())((acc, event) => event.request.key :: acc).reverse.mkString(" ")

  var dir: File = _

  before {
    dir = createTempDir
  }

  after {
    Path(dir).deleteRecursively(force = true)
    Thread.sleep(5)
  }

  var uberstore: SegmentedUberStore = _

  describe("upon initialization") {
    it("should set liveDir and readOnlyDirs properly for empty uberstores") {
      uberstore = SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration)
      assert(1 === uberstore.liveDir.getNextSeq)
      assert("1" === uberstore.liveDir.name)
      assert(Nil === uberstore.readOnlyDirs)
    }
    it("should set liveDir and readOnlyDirs properly for single uberdirs") {
      createSegment(dir, "1")
      uberstore = SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration)
      assert(1 === uberstore.liveDir.getNextSeq)
      assert("1" === uberstore.liveDir.name)
      assert(Nil === uberstore.readOnlyDirs)
    }
    it("should set liveDir and readOnlyDirs properly for multiple uberdirs") {
      createSegment(dir, "1")
      createSegment(dir, "5")
      createSegment(dir, "10")
      uberstore = SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration)
      assert(1 === uberstore.liveDir.getNextSeq)
      assert("10" === uberstore.liveDir.name)
      assert(List("1", "5") === uberstore.readOnlyDirs.map(_.name))
    }
  }

  describe("writeEvent") {
    it("should write to the live uberstore only") {
      createSegment(dir, "1")
      createSegment(dir, "5")
      createSegment(dir, "10")
      uberstore = SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration)
      uberstore.writeEntry(OrderedEvent(10L, 1L, Delete("1")))
      assert(1 === uberstore.readOnlyDirs(0).getNextSeq)
      // XXX uberdirs no longer base their nextSeq on the dir name, but rather the contents
      // we can stage this better once uberstore.split is implemented
      assert(1 === uberstore.readOnlyDirs(1).getNextSeq)
      assert(11 === uberstore.liveDir.getNextSeq)
    }

    it("should throw an illegalstateexception if we write out of order") {
      uberstore = SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration)
      uberstore.writeEntry(OrderedEvent(10L, 1L, Delete("1")))

      intercept[IllegalArgumentException] {
        uberstore.writeEntry(OrderedEvent(9L, 1L, Delete("1")))
      }
    }

    it("should update nextSeq based on the sequence number of the latest event") {
      uberstore = SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration)
      assert(1L === uberstore.getNextSeq)


      uberstore.writeEntry(OrderedEvent(999L, 1L, Delete("1")))
      assert(1000L === uberstore.getNextSeq)
    }

    it ("should split after the specified number of events have been written") {
      val siriusConfig = new SiriusConfiguration()
      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val underTest = new SegmentedUberStore(dir, 10L, segmentedCompactor)

      assert("1" === underTest.liveDir.name)
      writeUberStoreEvents(underTest, Range.inclusive(1, 10).toList)
      assert("2" === underTest.liveDir.name)
    }

  }

  describe("getNextSeq") {
    it("should reflect livedir's nextSeq") {
      createSegment(dir, "1")
      createSegment(dir, "5")
      createSegment(dir, "10")
      uberstore = SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration)
      assert(uberstore.liveDir.getNextSeq === uberstore.getNextSeq)
    }
  }

  describe("foldLeftRange") {
    it("should reflect livedir's nextSeq") {
      createSegment(dir, "1")
      createSegment(dir, "5")
      createSegment(dir, "10")
      uberstore = SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration)
      uberstore.writeEntry(OrderedEvent(10L, 1L, Delete("1")))
      uberstore.writeEntry(OrderedEvent(11L, 1L, Delete("2")))
      assert(List(Delete("1"), Delete("2")) ===
        uberstore.foldLeftRange(0, Long.MaxValue)(List[SiriusRequest]())(
          (acc, event) => event.request +: acc
        ).reverse
      )
    }
  }

  describe("close") {
    it("should close all of the associated uberdirs") {
      uberstore.close()
      uberstore.readOnlyDirs.foreach(
         uberstore => assert(true === uberstore.isClosed)
      )
      assert(true === uberstore.liveDir.isClosed)
    }
  }

  describe("compact") {
    it ("should perform a do-nothing compact of a single Segment, if |readOnlyDirs| == 1") {
      val siriusConfig = new SiriusConfiguration()
      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val underTest = new SegmentedUberStore(dir, 10L, segmentedCompactor)

      writeUberStoreEvents(underTest, Range.inclusive(1, 10).toList)
      underTest.compactAll()

      val segment = findSegment("1", underTest.readOnlyDirs)
      assert(10 === segment.size)
      assert(true === segment.isApplied)
    }

    it ("should perform a real compact of Segment 1 if Segment 2 is read-only") {
      val siriusConfig = new SiriusConfiguration()
      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val underTest = new SegmentedUberStore(dir, 10L, segmentedCompactor)

      writeUberStoreEvents(underTest, Range.inclusive(1, 10).toList)
      writeUberStoreEvents(underTest, Range.inclusive(6, 15).toList)
      // to trigger split
      writeUberStoreEvents(underTest, List(16))

      underTest.compactAll()

      assert("1 2 3 4 5" === listEvents(findSegment("1", underTest.readOnlyDirs)))
    }

    it ("should compact properly if presented with a number of non-compacted Segments") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val third = Segment(dir, "3")
      // now we've just split
      Segment(dir, "4")

      val segments = List(first, second, third)

      // all segments have same events
      segments.foreach(writeEvents(_, List(1L, 2L, 3L, 4L)))

      val underTest = SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration)
      underTest.compactAll()

      assert("4" === underTest.liveDir.name)

      val newFirst = findSegment("1", underTest.readOnlyDirs)
      val newSecond = findSegment("2", underTest.readOnlyDirs)
      val newThird = findSegment("3", underTest.readOnlyDirs)

      assert("" === listEvents(newFirst))
      assert(true === newFirst.isApplied)

      assert("" === listEvents(newSecond))
      assert(true === newSecond.isApplied)

      assert("1 2 3 4" === listEvents(newThird))
      assert(true === newThird.isApplied)
    }

    it ("should internally compact a single segment") {
      val first = Segment(dir, "1")
      Segment(dir, "2")

      List(
        OrderedEvent(1L, 0L, Delete("1")),
        OrderedEvent(2L, 0L, Delete("2")),
        OrderedEvent(3L, 0L, Delete("3")),
        OrderedEvent(4L, 0L, Delete("3")),
        OrderedEvent(5L, 0L, Delete("3")),
        OrderedEvent(6L, 0L, Delete("6"))
      ).foreach(first.writeEntry)

      val underTest = SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration)
      underTest.compactAll()

      val seqs = first.foldLeft(List[Long]())((acc, evt) => evt.sequence +: acc).reverse
      assert(List(1L, 2L, 5L, 6L) === seqs)
    }
  }

  describe("merge") {
    it("should do nothing if there are no mergeable segments") {
      createPopulatedSegment(dir, "1", Range.inclusive(1, 3).toList, isApplied = true)
      createPopulatedSegment(dir, "2", Range.inclusive(4, 6).toList, isApplied = true)
      createPopulatedSegment(dir, "3", Range.inclusive(7, 9).toList, isApplied = true)
      createSegment(dir, "4")

      val config = new SiriusConfiguration
      config.setProp(SiriusConfiguration.LOG_EVENTS_PER_SEGMENT, 5L)

      val underTest = SegmentedUberStore(dir.getAbsolutePath, config)
      assert(3 === underTest.readOnlyDirs.size)

      underTest.merge()
      assert(3 === underTest.readOnlyDirs.size)
    }
    it("should merge two segments if they are considered mergeable at the beginning") {
      createPopulatedSegment(dir, "1", Range.inclusive(1, 3).toList, isApplied = true)
      createPopulatedSegment(dir, "2", Range.inclusive(4, 6).toList, isApplied = true)
      createPopulatedSegment(dir, "3", Range.inclusive(7, 9).toList, isApplied = true)
      createSegment(dir, "4")

      val config = new SiriusConfiguration
      config.setProp(SiriusConfiguration.LOG_EVENTS_PER_SEGMENT, 6L)

      val underTest = SegmentedUberStore(dir.getAbsolutePath, config)
      assert(3 === underTest.readOnlyDirs.size)

      underTest.merge()
      assert(2 === underTest.readOnlyDirs.size)
      assert("1" === underTest.readOnlyDirs(0).name)
      assert("3" === underTest.readOnlyDirs(1).name)
    }
    it("should merge two segments if they are considered mergeable at the end") {
      createPopulatedSegment(dir, "1", Range.inclusive(1, 6).toList, isApplied = true)
      createPopulatedSegment(dir, "2", Range.inclusive(7, 9).toList, isApplied = true)
      createPopulatedSegment(dir, "3", Range.inclusive(10, 12).toList, isApplied = true)
      createSegment(dir, "4")

      val config = new SiriusConfiguration
      config.setProp(SiriusConfiguration.LOG_EVENTS_PER_SEGMENT, 6L)

      val underTest = SegmentedUberStore(dir.getAbsolutePath, config)
      assert(3 === underTest.readOnlyDirs.size)

      underTest.merge()
      assert(2 === underTest.readOnlyDirs.size)
      assert("1" === underTest.readOnlyDirs(0).name)
      assert("2" === underTest.readOnlyDirs(1).name)
    }
    it("should merge three consecutive segments that can be merged into one") {
      createPopulatedSegment(dir, "1", Range.inclusive(1, 3).toList, isApplied = true)
      createPopulatedSegment(dir, "2", Range.inclusive(4, 6).toList, isApplied = true)
      createPopulatedSegment(dir, "3", Range.inclusive(7, 9).toList, isApplied = true)
      createSegment(dir, "4")

      val config = new SiriusConfiguration
      config.setProp(SiriusConfiguration.LOG_EVENTS_PER_SEGMENT, 9L)

      val underTest = SegmentedUberStore(dir.getAbsolutePath, config)
      assert(3 === underTest.readOnlyDirs.size)

      underTest.merge()
      assert(1 === underTest.readOnlyDirs.size)
      assert("1" === underTest.readOnlyDirs(0).name)
    }
    it("should be able to merge all the segments into one if necessary") {}
  }

  describe("isMergeable") {
    it("should return false if the left segment has not been applied") {
      val left = Segment(dir, "1")
      val right = Segment(dir, "2")
      right.setApplied(applied = true)

      assert(false === SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration).isMergeable(left, right))
    }
    it("should return false if the right segment has not been applied") {
      val left = Segment(dir, "1")
      val right = Segment(dir, "2")
      left.setApplied(applied = true)

      assert(false === SegmentedUberStore(dir.getAbsolutePath, new SiriusConfiguration).isMergeable(left, right))
    }
    it("should return false if the combined size of the segments > eventsPerSegment") {
      val left = Segment(dir, "1")
      val right = Segment(dir, "2")
      left.setApplied(applied = true)
      right.setApplied(applied = true)

      val config = new SiriusConfiguration
      config.setProp(SiriusConfiguration.LOG_EVENTS_PER_SEGMENT, 5L)

      writeEvents(left, List(1L, 2L, 3L))
      writeEvents(right, List(4L, 5L, 6L))

      assert(false === SegmentedUberStore(dir.getAbsolutePath, config).isMergeable(left, right))
    }
    it("should return true if both are applied and combined size < eventsPerSegment") {
      val left = Segment(dir, "1")
      val right = Segment(dir, "2")
      left.setApplied(applied = true)
      right.setApplied(applied = true)

      val config = new SiriusConfiguration
      config.setProp(SiriusConfiguration.LOG_EVENTS_PER_SEGMENT, 5L)

      writeEvents(left, List(1L, 2L))
      writeEvents(right, List(4L, 5L))

      assert(true === SegmentedUberStore(dir.getAbsolutePath, config).isMergeable(left, right))
    }
    it("should return true if both are applied and combined size == eventsPerSegment") {
      val left = Segment(dir, "1")
      val right = Segment(dir, "2")
      left.setApplied(applied = true)
      right.setApplied(applied = true)

      val config = new SiriusConfiguration
      config.setProp(SiriusConfiguration.LOG_EVENTS_PER_SEGMENT, 5L)

      writeEvents(left, List(1L, 2L, 3L))
      writeEvents(right, List(4L, 5L))

      assert(true === SegmentedUberStore(dir.getAbsolutePath, config).isMergeable(left, right))
    }
  }

  describe("repair") {
    it("should properly handle the existence of both base and compacted") {
      createPopulatedSegment(dir, "1", List(1))
      createPopulatedSegment(dir, "1" + SegmentedCompactor.COMPACTING_SUFFIX, List(2))

      SegmentedUberStore.repair(dir.getAbsolutePath)
      assert("1" === listEvents(SegmentedUberStore(dir.getAbsolutePath)))
      assert(dir.listFiles().count(_.isDirectory) == 1)
    }
    it("should properly handle the existence of both temp and compacted") {
      createPopulatedSegment(dir, "1.temp", List(1))
      createPopulatedSegment(dir, "1" + SegmentedCompactor.COMPACTING_SUFFIX, List(2))

      SegmentedUberStore.repair(dir.getAbsolutePath)
      assert("2" === listEvents(SegmentedUberStore(dir.getAbsolutePath)))
      assert(dir.listFiles().count(_.isDirectory) == 1)
    }
    it("should properly handle the existence of both base and temp") {
      createPopulatedSegment(dir, "1", List(1))
      createPopulatedSegment(dir, "1" + SegmentedCompactor.TEMP_SUFFIX, List(2))

      SegmentedUberStore.repair(dir.getAbsolutePath)
      assert("1" === listEvents(SegmentedUberStore(dir.getAbsolutePath)))
      assert(dir.listFiles().count(_.isDirectory) == 1)
    }
    it("should do nothing if the SegmentedUberStore is proper") {
      createPopulatedSegment(dir, "1", List(1))
      createPopulatedSegment(dir, "2", List(2))

      SegmentedUberStore.repair(dir.getAbsolutePath)
      assert("1 2" === listEvents(SegmentedUberStore(dir.getAbsolutePath)))
      assert(dir.listFiles().count(_.isDirectory) == 2)
    }
    it("should do the expected things when all of the error cases appear at once") {
      createPopulatedSegment(dir, "1", List(1))
      createPopulatedSegment(dir, "2", List(2))
      createPopulatedSegment(dir, "2" + SegmentedCompactor.COMPACTING_SUFFIX, List(20))
      createPopulatedSegment(dir, "3" + SegmentedCompactor.TEMP_SUFFIX, List(30))
      createPopulatedSegment(dir, "3" + SegmentedCompactor.COMPACTING_SUFFIX, List(3))
      createPopulatedSegment(dir, "4", List(4))
      createPopulatedSegment(dir, "1" + SegmentedCompactor.TEMP_SUFFIX, List(40))
      createPopulatedSegment(dir, "5", List(5))

      SegmentedUberStore.repair(dir.getAbsolutePath)
      assert("1 2 3 4 5" === listEvents(SegmentedUberStore(dir.getAbsolutePath)))
      assert(dir.listFiles().count(_.isDirectory) == 5)
    }
  }

  describe("size"){
    it ("should report size correctly with 0 segments"){
      assert(0L === SegmentedUberStore(dir.getAbsolutePath).size)
    }

    it ("should report size correctly with 1 segments"){
      createPopulatedSegment(dir, "1", List(1))
      assert(65L === SegmentedUberStore(dir.getAbsolutePath).size)
    }

    it ("should report size correctly with multiple segments"){
      createPopulatedSegment(dir, "1", List(1))
      createPopulatedSegment(dir, "2", List(2))
      createPopulatedSegment(dir, "3", List(3))
      createPopulatedSegment(dir, "4", List(4))
      assert(260L === SegmentedUberStore(dir.getAbsolutePath).size)
    }
  }
}
