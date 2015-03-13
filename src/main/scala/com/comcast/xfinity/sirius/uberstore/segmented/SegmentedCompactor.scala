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

import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}

import scalax.file.Path
import java.io.File
import annotation.tailrec

object SegmentedCompactor {
  val COMPACTING_SUFFIX = ".compacting"
  val TEMP_SUFFIX = ".temp"

  def apply(siriusConfig: SiriusConfiguration) : SegmentedCompactor = {

    val maxDeleteAgeHours = siriusConfig.getInt(SiriusConfiguration.COMPACTION_MAX_DELETE_AGE_HOURS, Integer.MAX_VALUE)
    val maxDeleteAgeMillis = 60L * 60 * 1000 * maxDeleteAgeHours

    new SegmentedCompactor(maxDeleteAgeMillis)
  }
}

private [segmented] class SegmentedCompactor(maxDeleteAgeMillis: Long) {

  /**
   * Replaces one Segment file with another, removing the original.
   *
   * If original has not been closed, it will be. Ensure thread-safety before
   * calling this method.
   *
   * @param original Segment to be replaced
   * @param replacement full path of the replacement
   * @return new segment, ready for use, with the location of original and contents of replacement
   */
  def replace(original: Segment, replacement: String): Segment = {
    original.close()

    val originalPath = Path.fromString(original.location.getAbsolutePath)
    val replacementPath = Path.fromString(replacement)
    val tempPath = Path.fromString(
      new File(original.location.getParent, original.location.getName + SegmentedCompactor.TEMP_SUFFIX).getAbsolutePath
    )

    originalPath.moveTo(tempPath)
    replacementPath.moveTo(originalPath)
    tempPath.deleteRecursively()

    Segment(original.location.getParentFile, original.location.getName)
  }

  /**
   * If one exists, finds a segment in the supplied list that can be applied in compaction.
   *
   * @param segments list of all candidate segments
   * @return List of all Segments that could be compacted against
   */
  def findCompactableSegments(segments: List[Segment]): List[Segment] = segments.filterNot(_.isApplied)

  /**
   * Compact supplied list against a single segment. This method will filter out segments
   * that should not be compacted (i.e., segments in the list occurring after toCompact
   * in time)
   *
   * @param toCompactAgainst segment to compact against
   * @param allSegments list of segments to be compacted
   * @return Map of (old Segment -> compacted Segment location)
   */
  def compactAgainst(toCompactAgainst: Segment, allSegments: List[Segment]): Map[Segment, String] =
    compactSegments(toCompactAgainst, allSegments.filter(_.name.toLong < toCompactAgainst.name.toLong))

  /**
   * Compact all provided Segments against the provided Segment. There is no filtering in this method: only
   * provide the Segments you want compacted.
   *
   * @param toCompactAgainst Segment to compact against
   * @param segments segments to be compacted
   */
  private def compactSegments(toCompactAgainst: Segment, segments: List[Segment]): Map[Segment, String] = {
    val keys = toCompactAgainst.keys
    segments.foldLeft(Map[Segment, String]())(
      (map, toCompact) => {
        val compactInto = Segment(toCompact.location.getParentFile, toCompact.name + SegmentedCompactor.COMPACTING_SUFFIX)
        compactSegment(keys, toCompact, compactInto)

        compactInto.setApplied(toCompact.isApplied)
        compactInto.close()

        map + (toCompact -> compactInto.location.getAbsolutePath)
      })
  }

  /**
   * Compact a single Segment, according to the GC keys provided.  Deletes that are older than the maxDeleteAge
   * are purged.
   *
   * @param keys keys to gc against
   * @param source source segment, to be GC'ed
   * @param dest destination segment, empty, will be populated
   */
  private def compactSegment(keys: Set[String], source: Segment, dest: Segment) {

    // The earliest timestamp for a Delete to remain.  Deletes with an earlier timestamp will be purged.
    // By default maxDeleteAgeMillis is really big so no Deletes will be purged.
    val earliestDeleteTimestamp = System.currentTimeMillis - maxDeleteAgeMillis

    source.foreach {
      case OrderedEvent(_, timestamp, Delete(key)) if timestamp < earliestDeleteTimestamp => // nothing
      case event if !keys.contains(event.request.key) => dest.writeEntry(event)
      case _ => // this event is overridden by some future event: garbage collect it.
    }
  }

  /**
   * Find the next two consecutive Segments that may be merged and optionally return them.
   *
   * @param segments list of input segments
   * @param isMergeable predicate to determine mergeability
   * @return optional tuple2 of mergeable segments
   */
  @tailrec
  final def findNextMergeableSegments(segments: List[Segment], isMergeable: (Segment, Segment) => Boolean): Option[(Segment, Segment)] = {
    segments.take(2) match {
      case list if list.size < 2 => None
      case list if isMergeable(list(0), list(1)) => Some(list(0), list(1))
      case _ => findNextMergeableSegments(segments.tail, isMergeable)
    }
  }

  /**
   * Delete the files that represent this segment on disk.
   *
   * Please be careful with this.
   *
   * @param segment segment to permanently delete
   */
  def delete(segment: Segment) {
    segment.close()
    Path.fromString(segment.location.getAbsolutePath).deleteRecursively()
  }

  /**
   * Write the events from two Segments into a third, in order.
   *
   * @param left first source segment
   * @param right second source segment
   * @param targetFile location where to write the files. This probably should not already exist.
   */
  def mergeSegments(left: Segment, right: Segment, targetFile: File) {
    val target = Segment(targetFile)
    left.foreach(target.writeEntry)
    right.foreach(target.writeEntry)
    target.setApplied(applied = left.isApplied && right.isApplied)
    target.close()
  }

}

