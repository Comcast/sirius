package com.comcast.xfinity.sirius.uberstore.segmented

import com.comcast.xfinity.sirius.NiceTest
import java.io.File
import scalax.file.Path
import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}
import org.mockito.Mockito._

class SegmentedCompactorTest extends NiceTest with BeforeAndAfterAll {

  def createTempDir: File = {
    val tempDirName = "%s/compactor-itest-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    val dir = new File(tempDirName)
    dir.mkdirs()
    dir
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
  }

  describe("replace") {
    it("should replace the contents of the original segment with those of the replacement") {
      val original = Segment(dir, "1")
      val replacement = Segment(dir, "1.compacted")
      val foldFun = (acc: List[Long], evt: OrderedEvent) => evt.sequence :: acc

      writeEvents(original, List(1L, 2L, 3L, 4L))
      writeEvents(replacement, List(2L, 3L))

      val expected = replacement.foldLeft(List[Long]())(foldFun)

      val result = SegmentedCompactor.replace(original, replacement.location.getAbsolutePath)

      assert(original.location.getAbsolutePath === result.location.getAbsolutePath)
      assert(expected === result.foldLeft(List[Long]())(foldFun))
    }

    it("should handle an empty replacement normally") {
      val original = Segment(dir, "1")
      val replacement = Segment(dir, "1.compacted")
      val foldFun = (acc: List[Long], evt: OrderedEvent) => evt.sequence :: acc

      writeEvents(original, List(1L, 2L, 3L, 4L))

      val expected = List[Long]()

      val result = SegmentedCompactor.replace(original, replacement.location.getAbsolutePath)

      assert(original.location.getAbsolutePath === result.location.getAbsolutePath)
      assert(expected === result.foldLeft(List[Long]())(foldFun))
    }
  }

  describe("findCompactableSegments") {
    it("should return an empty List if no segments are provided") {
      val list = List[Segment]()
      assert(List() === SegmentedCompactor.findCompactableSegments(list))
    }

    it("should return an empty List if all segments have been applied") {
      val mockSegment = mock[Segment]
      val list = List(mockSegment)

      doReturn(true).when(mockSegment).isApplied

      assert(List() === SegmentedCompactor.findCompactableSegments(list))
    }

    it("should return only the segments that have not been applied if there are some") {
      val appliedSegment = mock[Segment]
      val notAppliedSegment = mock[Segment]
      val list = List(appliedSegment, notAppliedSegment)

      doReturn(true).when(appliedSegment).isApplied
      doReturn(false).when(notAppliedSegment).isApplied

      assert(List(notAppliedSegment) === SegmentedCompactor.findCompactableSegments(list))
    }
  }

  describe("compactAgainst") {
    it("should preserve a false isApplied flag when a segment is compacted") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val segments = List(first, second)
      segments.foreach(writeEvents(_, List(1L, 2L, 3L, 4L)))

      first.setApplied(applied = false)
      val compactionMap = SegmentedCompactor.compactAgainst(second, segments)
      assert(false === makeSegment(compactionMap(first)).isApplied)
    }

    it("should preserve a true isApplied flag when a segment is compacted") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val segments = List(first, second)
      segments.foreach(writeEvents(_, List(1L, 2L, 3L, 4L)))

      first.setApplied(applied = true)
      val compactionMap = SegmentedCompactor.compactAgainst(second, segments)
      assert(true === makeSegment(compactionMap(first)).isApplied)
    }

    it("should compact nothing if the supplied segment is the first in sort order") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val third = Segment(dir, "3")
      val segments = List(first, second, third)

      segments.foreach(writeEvents(_, List(1L, 2L, 3L, 4L)))

      val compactionMap = SegmentedCompactor.compactAgainst(first, segments)

      assert(None == compactionMap.get(first))
      assert(None == compactionMap.get(second))
      assert(None == compactionMap.get(third))
    }

    it("should only compact Segments with name < supplied segment's name") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val third = Segment(dir, "3")
      val segments = List(first, second, third)

      segments.foreach(writeEvents(_, List(1L, 2L, 3L, 4L)))

      val compactionMap = SegmentedCompactor.compactAgainst(second, segments)

      assert(None != compactionMap.get(first))
      assert(None == compactionMap.get(second))
      assert(None == compactionMap.get(third))
    }

    it("should compact all other segments if the supplied segment is the latest") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val third = Segment(dir, "3")
      val segments = List(first, second, third)

      segments.foreach(writeEvents(_, List(1L, 2L, 3L, 4L)))

      val compactionMap = SegmentedCompactor.compactAgainst(third, segments)

      assert(None != compactionMap.get(first))
      assert(None != compactionMap.get(second))
      assert(None == compactionMap.get(third))
    }

    it("should compact all other segments if the supplied segment is the latest, regardless of list order") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val third = Segment(dir, "3")
      val segments = List(third, second, first)

      segments.foreach(writeEvents(_, List(1L, 2L, 3L, 4L)))

      val compactionMap = SegmentedCompactor.compactAgainst(third, segments)

      assert(None != compactionMap.get(first))
      assert(None != compactionMap.get(second))
      assert(None == compactionMap.get(third))
    }

    it("should remove all events in appropriate segments that are overridden in the supplied segment when SOME events should be removed") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val segments = List(first, second)

      writeEvents(first, List(1L, 2L, 3L, 4L))
      writeEvents(second, List(3L, 4L, 5L, 6L))

      val compactionMap = SegmentedCompactor.compactAgainst(second, segments)
      val compacted = makeSegment(compactionMap(first))

      assert("1 2" === listEvents(compacted))
    }

    it("should remove all events in appropriate segments that are overridden in the supplied segment when ALL events should be removed") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val segments = List(first, second)

      writeEvents(first, List(1L, 2L, 3L, 4L))
      writeEvents(second, List(1L, 2L, 3L, 4L))

      val compactionMap = SegmentedCompactor.compactAgainst(second, segments)
      val compacted = makeSegment(compactionMap(first))

      assert(List(first) === compactionMap.keys.toList)
      assert("" === listEvents(compacted))
    }
  }
}
