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
import com.comcast.xfinity.sirius.api.SiriusConfiguration

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

  def writeEvents(segment: Segment, events: List[Long], timestamp: Long = 0L) {
    for (i <- events) {
      segment.writeEntry(OrderedEvent(i, timestamp, Delete(i.toString)))
    }
  }

  def makeSegment(fullPath: String): Segment = {
    val file = new File(fullPath)
    Segment(file.getParentFile, file.getName)
  }

  def listEvents(segment: Segment) =
    segment.foldLeft(List[String]())((acc, event) => event.request.key :: acc).reverse.mkString(" ")

  var dir: File = _

  val siriusConfig = new SiriusConfiguration()

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

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val result = segmentedCompactor.replace(original, replacement.location.getAbsolutePath)

      assert(original.location.getAbsolutePath === result.location.getAbsolutePath)
      assert(expected === result.foldLeft(List[Long]())(foldFun))
    }

    it("should handle an empty replacement normally") {
      val original = Segment(dir, "1")
      val replacement = Segment(dir, "1.compacted")
      val foldFun = (acc: List[Long], evt: OrderedEvent) => evt.sequence :: acc

      writeEvents(original, List(1L, 2L, 3L, 4L))

      val expected = List[Long]()

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val result = segmentedCompactor.replace(original, replacement.location.getAbsolutePath)

      assert(original.location.getAbsolutePath === result.location.getAbsolutePath)
      assert(expected === result.foldLeft(List[Long]())(foldFun))
    }
  }

  describe("findCompactableSegments") {
    it("should return an empty List if no segments are provided") {
      val list = List[Segment]()
      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      assert(List() === segmentedCompactor.findCompactableSegments(list))
    }

    it("should return an empty List if all segments have been applied") {
      val mockSegment = mock[Segment]
      val list = List(mockSegment)

      doReturn(true).when(mockSegment).isApplied

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      assert(List() === segmentedCompactor.findCompactableSegments(list))
    }

    it("should return only the segments that have not been applied if there are some") {
      val appliedSegment = mock[Segment]
      val notAppliedSegment = mock[Segment]
      val list = List(appliedSegment, notAppliedSegment)

      doReturn(true).when(appliedSegment).isApplied
      doReturn(false).when(notAppliedSegment).isApplied

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      assert(List(notAppliedSegment) === segmentedCompactor.findCompactableSegments(list))
    }
  }

  describe("compactInternally") {
    it("should remove earlier entries for duplicate keys within the segment") {
      val segment = Segment(dir, "1")
      List(
        OrderedEvent(1L, 0L, Delete("1")),
        OrderedEvent(2L, 0L, Delete("2")),
        OrderedEvent(3L, 0L, Delete("2")),
        OrderedEvent(4L, 0L, Delete("2")),
        OrderedEvent(5L, 0L, Delete("5"))
      ).foreach(segment.writeEntry)

      val underTest = SegmentedCompactor(new SiriusConfiguration())
      val compacted = makeSegment(underTest.compactInternally(segment))

      val seqs = compacted.foldLeft(List[Long]())((acc, evt) => evt.sequence +: acc).reverse
      assert(List(1L, 4L, 5L) === seqs)
      assert("1 2 5" === listEvents(compacted))
    }
    it("should do nothing if there are no duplicate keys") {
      val segment = Segment(dir, "1")
      writeEvents(segment, List(1L, 2L, 3L, 4L))

      val underTest = SegmentedCompactor(new SiriusConfiguration())
      val compacted = makeSegment(underTest.compactInternally(segment))
      assert("1 2 3 4" === listEvents(compacted))
    }
    it("should set the internallyCompacted flag to true") {
      val segment = Segment(dir, "1")

      val underTest = SegmentedCompactor(new SiriusConfiguration())
      val compacted = makeSegment(underTest.compactInternally(segment))

      assert(true === compacted.isInternallyCompacted)
    }
    it("should preserve the keys-applied flag") {
      val segment = Segment(dir, "1")
      segment.setApplied(applied = true)

      val underTest = SegmentedCompactor(new SiriusConfiguration())
      val compacted = makeSegment(underTest.compactInternally(segment))

      assert(true === compacted.isApplied)
    }
  }

  describe("findInternalCompactionCandidates") {
    it("should return an empty list if all segments are internally compacted") {
      val (one, two, three) = (Segment(dir, "1"), Segment(dir, "2"), Segment(dir, "3"))
      one.setInternallyCompacted(compacted = true)
      two.setInternallyCompacted(compacted = true)
      three.setInternallyCompacted(compacted = true)

      val underTest = SegmentedCompactor(new SiriusConfiguration())
      val candidates = underTest.findInternalCompactionCandidates(List(one, two, three))

      assert(List() === candidates)
    }
    it("should return only the segments that have not been internally compacted") {
      val (one, two, three) = (Segment(dir, "1"), Segment(dir, "2"), Segment(dir, "3"))
      one.setInternallyCompacted(compacted = true)
      two.setInternallyCompacted(compacted = false)
      three.setInternallyCompacted(compacted = true)

      val underTest = SegmentedCompactor(new SiriusConfiguration())
      val candidates = underTest.findInternalCompactionCandidates(List(one, two, three))

      assert(List(two) === candidates)
    }
  }

  describe("compactAgainst") {
    it("should fail if one of the segments to compact has not been internally compacted") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val segments = List(first)

      first.setInternallyCompacted(compacted = false)

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      intercept[IllegalStateException] {
        segmentedCompactor.compactAgainst(second, segments)
      }
    }
    it("should preserve a false isApplied flag when a segment is compacted") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val segments = List(first, second)
      segments.foreach(writeEvents(_, List(1L, 2L, 3L, 4L)))

      first.setApplied(applied = false)
      first.setInternallyCompacted(compacted = true)
      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val compactionMap = segmentedCompactor.compactAgainst(second, segments)
      assert(false === makeSegment(compactionMap(first)).isApplied)
    }

    it("should preserve a true isApplied flag when a segment is compacted") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val segments = List(first, second)
      segments.foreach(writeEvents(_, List(1L, 2L, 3L, 4L)))

      first.setApplied(applied = true)
      first.setInternallyCompacted(compacted = true)
      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val compactionMap = segmentedCompactor.compactAgainst(second, segments)
      assert(true === makeSegment(compactionMap(first)).isApplied)
    }

    it("should compact nothing if the supplied segment is the first in sort order") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val third = Segment(dir, "3")
      val segments = List(first, second, third)

      segments.foreach(writeEvents(_, List(1L, 2L, 3L, 4L)))

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val compactionMap = segmentedCompactor.compactAgainst(first, segments)

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
      segments.foreach(_.setInternallyCompacted(compacted = true))

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val compactionMap = segmentedCompactor.compactAgainst(second, segments)

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
      segments.foreach(_.setInternallyCompacted(compacted = true))

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val compactionMap = segmentedCompactor.compactAgainst(third, segments)

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
      segments.foreach(_.setInternallyCompacted(compacted = true))

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val compactionMap = segmentedCompactor.compactAgainst(third, segments)

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
      segments.foreach(_.setInternallyCompacted(compacted = true))

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val compactionMap = segmentedCompactor.compactAgainst(second, segments)
      val compacted = makeSegment(compactionMap(first))

      assert("1 2" === listEvents(compacted))
    }

    it("should remove all events in appropriate segments that are overridden in the supplied segment when ALL events should be removed") {
      val first = Segment(dir, "1")
      val second = Segment(dir, "2")
      val segments = List(first, second)

      writeEvents(first, List(1L, 2L, 3L, 4L))
      writeEvents(second, List(1L, 2L, 3L, 4L))
      segments.foreach(_.setInternallyCompacted(compacted = true))

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      val compactionMap = segmentedCompactor.compactAgainst(second, segments)
      val compacted = makeSegment(compactionMap(first))

      assert(List(first) === compactionMap.keys.toList)
      assert("" === listEvents(compacted))
    }
  }

  it("should remove all Delete events in appropriate segments that are older than COMPACTION_MAX_DELETE_AGE_HOURS") {
    val first = Segment(dir, "1")
    val second = Segment(dir, "2")
    val segments = List(first, second)

    val now = System.currentTimeMillis()
    val twoHoursAgo = now - 2L * 60 * 60 * 1000

    writeEvents(first, List(1L), twoHoursAgo) // This one should be compacted out due to age.
    writeEvents(first, List(2L, 3L, 4L), now)
    writeEvents(second, List(3L, 4L, 5L, 6L), now)
    segments.foreach(_.setInternallyCompacted(compacted = true))

    val siriusConfigWithMaxAge = new SiriusConfiguration()
    siriusConfigWithMaxAge.setProp(SiriusConfiguration.COMPACTION_MAX_DELETE_AGE_HOURS, 1)
    val segmentedCompactor = SegmentedCompactor(siriusConfigWithMaxAge)
    val compactionMap = segmentedCompactor.compactAgainst(second, segments)

    val compacted = makeSegment(compactionMap(first))

    assert("2" === listEvents(compacted))
  }

  it("should not remove any Delete events when COMPACTION_MAX_DELETE_AGE_HOURS is bigger than the current time") {
    val first = Segment(dir, "1")
    val second = Segment(dir, "2")
    val segments = List(first, second)

    val now = System.currentTimeMillis()
    val twoHoursAgo = now - 2L * 60 * 60 * 1000

    writeEvents(first, List(1L), twoHoursAgo)
    writeEvents(first, List(2L, 3L, 4L), now)
    writeEvents(second, List(3L, 4L, 5L, 6L), now)
    segments.foreach(_.setInternallyCompacted(compacted = true))

    val siriusConfigWithMaxAge = new SiriusConfiguration()
    val nowInHours = now / (60L * 60 * 1000)
    val biggerThanNowInHours = nowInHours + 168 // One week bigger than now, in hours
    siriusConfigWithMaxAge.setProp(SiriusConfiguration.COMPACTION_MAX_DELETE_AGE_HOURS, biggerThanNowInHours)

    val segmentedCompactor = SegmentedCompactor(siriusConfigWithMaxAge)
    val compactionMap = segmentedCompactor.compactAgainst(second, segments)

    val compactedFirst = makeSegment(compactionMap(first))
    assert("1 2" === listEvents(compactedFirst))
  }

  describe("findNextMergeableSegments") {
    it("should do nothing if the input is of size 0 or 1") {
      val isMergeable = (left: Segment, right: Segment) => true
      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      assert(None === segmentedCompactor.findNextMergeableSegments(List(), isMergeable))
      assert(None === segmentedCompactor.findNextMergeableSegments(List(Segment(dir, "1")), isMergeable))
    }

    it("should return the first two elements if the return true for the isMergeable predicate") {
      val isMergeable = (left: Segment, right: Segment) => true
      val (one, two, three) = (Segment(dir, "1"), Segment(dir, "2"), Segment(dir, "3"))
      val segments = List(one, two, three)

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      assert(Some(one, two) === segmentedCompactor.findNextMergeableSegments(segments, isMergeable))
    }
    it("should return None if there are no mergeable elements in the list") {
      val isMergeable = (left: Segment, right: Segment) => false
      val (one, two, three) = (Segment(dir, "1"), Segment(dir, "2"), Segment(dir, "3"))
      val segments = List(one, two, three)

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      assert(None === segmentedCompactor.findNextMergeableSegments(segments, isMergeable))
    }
    it("should return the first mergeable elements in the list if there are any") {
      val isMergeable = (left: Segment, right: Segment) => left.name == "2" && right.name == "3"
      val (one, two, three) = (Segment(dir, "1"), Segment(dir, "2"), Segment(dir, "3"))
      val segments = List(one, two, three)

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      assert(Some(two, three) === segmentedCompactor.findNextMergeableSegments(segments, isMergeable))
    }
  }
  describe("delete") {
    it("should close the input segment") {
      val segment = Segment(dir, "1")
      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      segmentedCompactor.delete(segment)
      assert(true === segment.isClosed)
    }
    it("should remove the location of segment from the filesystem") {
      val segment = Segment(dir, "1")
      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      segmentedCompactor.delete(segment)
      assert(false === segment.location.exists())
    }
  }
  describe("mergeSegments") {
    it("should create a new segment at targetFile") {
      val (left, right) = (Segment(dir, "1"), Segment(dir, "2"))
      val target = new File(dir, "1-2.merged")

      assert(false === target.exists())
      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      segmentedCompactor.mergeSegments(left, right, target)
      assert(true === target.exists())
    }
    it("should write all of the elements from left and right into target, in order") {
      val (left, right) = (Segment(dir, "1"), Segment(dir, "2"))
      writeEvents(left, List(1L, 2L, 3L))
      writeEvents(right, List(4L, 5L, 6L))
      val target = new File(dir, "1-2.merged")

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      segmentedCompactor.mergeSegments(left, right, target)

      assert("1 2 3 4 5 6" === listEvents(Segment(target)))
    }
    it("should set the new segment's isApplied correctly if both left and right have been applied") {
      val (left, right) = (Segment(dir, "1"), Segment(dir, "2"))
      left.setApplied(applied = true)
      right.setApplied(applied = true)
      val target = new File(dir, "1-2.merged")

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      segmentedCompactor.mergeSegments(left, right, target)

      assert(true === Segment(target).isApplied)
    }
    it("should set the new segment's isApplied correctly if either left or right have not been applied") {
      val (left, right) = (Segment(dir, "1"), Segment(dir, "2"))
      left.setApplied(applied = false)
      right.setApplied(applied = true)
      val target = new File(dir, "1-2.merged")

      val segmentedCompactor = SegmentedCompactor(siriusConfig)
      segmentedCompactor.mergeSegments(left, right, target)

      assert(false === Segment(target).isApplied)
    }
    it("should set the new segment's isInternallyCompacted correctly") {
      def createMerged(leftFile: File, rightFile: File, target: File)
                      (leftApplied: Boolean, leftCompacted: Boolean,
                       rightApplied: Boolean, rightCompacted: Boolean): Segment = {
        Path(leftFile).deleteRecursively()
        Path(rightFile).deleteRecursively()
        Path(target).deleteRecursively()

        val left = Segment(leftFile)
        val right = Segment(rightFile)

        left.setApplied(leftApplied)
        left.setInternallyCompacted(leftCompacted)
        right.setApplied(rightApplied)
        right.setInternallyCompacted(rightCompacted)

        val segmentedCompactor = SegmentedCompactor(new SiriusConfiguration())
        segmentedCompactor.mergeSegments(left, right, target)
        Segment(target)
      }

      val (leftFile, rightFile) = (new File(dir, "1"), new File(dir, "2"))
      val target = new File(dir, "1-2.merged")
      val merge = createMerged(leftFile, rightFile, target) _

      // should only be true if both segments have been applied and internally compacted
      for (leftApplied <- List(true, false);
           leftCompacted <- List(true, false);
           rightApplied <- List(true, false);
           rightCompacted <- List(true, false)) {
        val expected = leftApplied && leftCompacted && rightApplied && rightCompacted
        assert(expected === merge(leftApplied, leftCompacted, rightApplied, rightCompacted).isInternallyCompacted,
               "Did not get expected value for isInternallyCompacted after merge")
      }
    }
  }
}
