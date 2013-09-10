package com.comcast.xfinity.sirius.uberstore.segmented

import com.comcast.xfinity.sirius.NiceTest
import java.io.File
import com.comcast.xfinity.sirius.api.impl._
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.Delete
import scalax.file.Path

class SegmentedUberStoreTest extends NiceTest {

  def createTempDir = {
    val tempDirName = "%s/uberstore-itest-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    val dir = new File(tempDirName)
    dir.mkdirs()
    dir
  }

  def createFakeSegment(baseDir: File, seq: String): File = {
    val dir = new File(baseDir, seq)
    dir.mkdir

    new File(dir, "index").createNewFile()
    new File(dir, "data").createNewFile()
    dir
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

  var dir: File = _

  before {
    dir = createTempDir
  }

  after {
    Path.fromString(dir.getAbsolutePath).deleteRecursively(force = true)
    Thread.sleep(5)
  }

  var uberstore: SegmentedUberStore = _

  describe("upon initialization") {
    it("should set liveDir and readOnlyDirs properly for empty uberstores") {
      uberstore = SegmentedUberStore(dir.getAbsolutePath)
      assert(1 === uberstore.liveDir.getNextSeq)
      assert("1" === uberstore.liveDir.name)
      assert(Nil === uberstore.readOnlyDirs)
    }
    it("should set liveDir and readOnlyDirs properly for single uberdirs") {
      createFakeSegment(dir, "1")
      uberstore = SegmentedUberStore(dir.getAbsolutePath)
      assert(1 === uberstore.liveDir.getNextSeq)
      assert("1" === uberstore.liveDir.name)
      assert(Nil === uberstore.readOnlyDirs)
    }
    it("should set liveDir and readOnlyDirs properly for multiple uberdirs") {
      createFakeSegment(dir, "1")
      createFakeSegment(dir, "5")
      createFakeSegment(dir, "10")
      uberstore = SegmentedUberStore(dir.getAbsolutePath)
      assert(1 === uberstore.liveDir.getNextSeq)
      assert("10" === uberstore.liveDir.name)
      assert(List("1", "5") === uberstore.readOnlyDirs.map(_.name))
    }
  }

  describe("writeEvent") {
    it("should write to the live uberstore only") {
      createFakeSegment(dir, "1")
      createFakeSegment(dir, "5")
      createFakeSegment(dir, "10")
      uberstore = SegmentedUberStore(dir.getAbsolutePath)
      uberstore.writeEntry(OrderedEvent(10L, 1L, Delete("1")))
      assert(1 === uberstore.readOnlyDirs(0).getNextSeq)
      // XXX uberdirs no longer base their nextSeq on the dir name, but rather the contents
      // we can stage this better once uberstore.split is implemented
      assert(1 === uberstore.readOnlyDirs(1).getNextSeq)
      assert(11 === uberstore.liveDir.getNextSeq)
    }

    it("should throw an illegalstateexception if we write out of order") {
      uberstore = SegmentedUberStore(dir.getAbsolutePath)
      uberstore.writeEntry(OrderedEvent(10L, 1L, Delete("1")))

      intercept[IllegalArgumentException] {
        uberstore.writeEntry(OrderedEvent(9L, 1L, Delete("1")))
      }
    }

    it("should update nextSeq based on the sequence number of the latest event") {
      uberstore = SegmentedUberStore(dir.getAbsolutePath)
      assert(1L === uberstore.getNextSeq)


      uberstore.writeEntry(OrderedEvent(999L, 1L, Delete("1")))
      assert(1000L === uberstore.getNextSeq)
    }

    it ("should split after the specified number of events have been written") {
      val underTest = new SegmentedUberStore(dir, 10L)

      assert("1" === underTest.liveDir.name)
      writeUberStoreEvents(underTest, Range.inclusive(1, 10).toList)
      assert("2" === underTest.liveDir.name)
    }

  }

  describe("getNextSeq") {
    it("should reflect livedir's nextSeq") {
      createFakeSegment(dir, "1")
      createFakeSegment(dir, "5")
      createFakeSegment(dir, "10")
      uberstore = SegmentedUberStore(dir.getAbsolutePath)
      assert(uberstore.liveDir.getNextSeq === uberstore.getNextSeq)
    }
  }

  describe("foldLeftRange") {
    it("should reflect livedir's nextSeq") {
      createFakeSegment(dir, "1")
      createFakeSegment(dir, "5")
      createFakeSegment(dir, "10")
      uberstore = SegmentedUberStore(dir.getAbsolutePath)
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
      val underTest = new SegmentedUberStore(dir, 10L)

      writeUberStoreEvents(underTest, Range.inclusive(1, 10).toList)
      underTest.compact()

      val segment = findSegment("1", underTest.readOnlyDirs)
      assert(10 === segment.size)
      assert(true === segment.isApplied)
    }

    it ("should perform a real compact of Segment 1 if Segment 2 is read-only") {
      val underTest = new SegmentedUberStore(dir, 10L)

      writeUberStoreEvents(underTest, Range.inclusive(1, 10).toList)
      writeUberStoreEvents(underTest, Range.inclusive(6, 15).toList)
      // to trigger split
      writeUberStoreEvents(underTest, List(16))

      underTest.compact()

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

      val underTest = SegmentedUberStore(dir.getAbsolutePath)
      underTest.compact()

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
  }
}
