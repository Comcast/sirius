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

import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import java.io.File
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import annotation.tailrec
import scalax.file.Path

object SegmentedUberStore {

  def versionId: String = "SEGMENTED-1.0"

  def init(location: String) {
    val dir = new File(location)
    dir.mkdirs()

    new File(dir, SegmentedUberStore.versionId).createNewFile()
  }

  /**
   * Repair a SegmentedUberStore at location. Handles possibles errors due to
   * incomplete compaction.
   *
   * @param location location of SegmentedUberStore
   */
  def repair(location: String) {
    val dir = new File(location)

    val baseNames = dir.listFiles.filter(_.getName.contains(".")).foldLeft(Set[String]())(
      (acc, file) =>  acc + file.getName.substring(0, file.getName.indexOf('.'))
    )

    baseNames.foreach(baseName => {
      val baseFile = new File(dir, baseName)
      val tempFile = new File(dir, baseName + SegmentedCompactor.TEMP_SUFFIX)
      val compactedFile = new File(dir, baseName + SegmentedCompactor.COMPACTING_SUFFIX)

      if (baseFile.exists && compactedFile.exists) {
        // compacted exists, base exists: incomplete compaction, delete compacted
        Path(compactedFile).deleteRecursively()
      } else if (tempFile.exists && compactedFile.exists) {
        // compacted exists, temp exists: incomplete replace, mv compacted base
        Path(compactedFile).moveTo(Path(baseFile))
        Path(tempFile).deleteRecursively()
      } else if (baseFile.exists && tempFile.exists) {
        // base exists, temp exists: incomplete replace, rm temp
        Path(tempFile).deleteRecursively()
      }
    })
  }

  /**
   * Create an SegmentedUberStore based in base.  base is NOT
   * created, it must exist. The files within base will
   * be created if they do not exist however.
   *
   * @param base directory to base the SegmentedUberStore in
   *
   * @return an instantiated SegmentedUberStore
   */
  def apply(base: String, siriusConfig: SiriusConfiguration = new SiriusConfiguration): SegmentedUberStore = {
    val MAX_EVENTS_PER_SEGMENT = siriusConfig.getProp(SiriusConfiguration.LOG_EVENTS_PER_SEGMENT, 1000000L)

    if (!new File(base, versionId).exists) {
      throw new IllegalStateException("Cannot start. Configured to boot with storage: %s, which is not found in %s".format(versionId, base))
    }

    repair(base)

    val segmentedCompactor = SegmentedCompactor(siriusConfig)

    new SegmentedUberStore(new File(base), MAX_EVENTS_PER_SEGMENT, segmentedCompactor)
  }
}

/**
 * Expectedly high performance sequence number based append only
 * storage.
 *
 * @param base directory SegmentedUberStore is based in
 */
class SegmentedUberStore private[segmented] (base: File, eventsPerSegment: Long, segmentedCompactor: SegmentedCompactor) extends SiriusLog {

  val replaceLock = new Object()
  val compactLock = new Object()

  // XXX incorporate maxDeleteAge, pass it in as a configuration option, not System.getProperty...
  // val maxDeleteAge = System.getProperty("maxDeleteAge", "604800000").toLong
  var readOnlyDirs: List[Segment] = _
  var liveDir: Segment = _
  var nextSeq: Long = _
  init()

  /**
   * @inheritdoc
   */
  def writeEntry(event: OrderedEvent) {
    if (event.sequence < nextSeq) {
      throw new IllegalArgumentException("May not write event out of order, expected sequence " + nextSeq + " but received " + event.sequence)
    }
    liveDir.writeEntry(event)
    nextSeq = event.sequence + 1

    if (liveDir.size >= eventsPerSegment) {
      split()
    }
  }

  /**
   * @inheritdoc
   */
  def getNextSeq = nextSeq

  /**
   * @inheritdoc
   */
  def foldLeftRange[T](startSeq: Long, endSeq: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T = replaceLock.synchronized {
    val res0 = readOnlyDirs.foldLeft(acc0)(
      (acc, dir) => dir.foldLeftRange(startSeq, endSeq)(acc)(foldFun)
    )
    liveDir.foldLeftRange(startSeq, endSeq)(res0)(foldFun)
  }


  /**
   * Close underlying file handles or connections.  This SegmentedUberStore should not be used after
   * close is called.
   */
  def close() {
    liveDir.close()
    readOnlyDirs.foreach(_.close())
  }

  /**
   * Determine whether this SegmentedUberStore is closed. If it's closed, it may not be written to.
   *
   * @return whether this is "closed," i.e., unable to be written to
   */
  def isClosed = liveDir.isClosed

  /**
   * Initialize liveDir, readOnlyDir, and nextSeq
   */
  private def init() {
    val segments = listSegments(base)

    groupSegments(base, segments) match {
      case (live, readOnly) =>
        liveDir = live
        readOnlyDirs = readOnly
    }

    nextSeq = (liveDir.getNextSeq :: readOnlyDirs.map(_.getNextSeq)).max
  }

  /**
   * From a list of segments, find the liveDir and the inactiveDirs
   * @param segments all segments (assumed unsorted)
   * @return tuple2 of (live segment, inactive segments)
   */
  private def groupSegments(location: File, segments: List[String]): (Segment, List[Segment]) = {
    segments.sortWith(_.toLong > _.toLong) match {
      case Nil => (Segment(location, "1"), Nil)
      case hd :: tl => (Segment(location, hd), tl.reverse.map(Segment(location, _)))
    }
  }

  /**
   * List segments in a directory.
   *
   * @param location root directory of uberstore
   * @return unsorted list of valid Segment names in location
   */
  private def listSegments(location: File): List[String] =
    location.listFiles.toList.collect {
      case f if f.isDirectory && f.getName.forall(_.isDigit) => f.getName
    }

  /**
   * For each unapplied Segment, compact previous segments according to its keys,
   * continuing as long as there are unapplied segments. When Segments have been
   * applied, merge any adjacent undersized segments.
   */
  def compact() = compactLock.synchronized {
    compactAll()
    merge()
  }

  /**
   * Calculates size of the SiriusLog.
   *
   * @return a measure of size of the SiriusLog
   */
  def size: Long = {
    def recursiveListFiles(f: File): Array[File] = {
      val these = f.listFiles
      these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
    }
    recursiveListFiles(base).filter(_.isFile).map(_.length).sum
  }

  /**
   * Compact readOnlyDirs until they can be compacted no longer.
   */
  private[segmented] def compactAll() {
    for (toCompact <- segmentedCompactor.findCompactableSegments(readOnlyDirs)) {
      val compactionMap = segmentedCompactor.compactAgainst(toCompact, readOnlyDirs)
      replaceSegments(compactionMap) // mutates readOnlyDirs
      toCompact.setApplied(applied = true)
    }
  }

  /**
   * Decide whether two segments may be merged.
   *
   * Segments must both have been applied in compaction, and must combined have
   * fewer than or equal to eventsPerSegment events.
   */
  private[segmented] val isMergeable =
    (left: Segment, right: Segment) =>
     left.size + right.size <= eventsPerSegment && left.isApplied && right.isApplied

  /**
   * Merge readOnlyDirs until they can be merged no longer.
   */
  @tailrec
  private[segmented] final def merge() {
    segmentedCompactor.findNextMergeableSegments(readOnlyDirs, isMergeable) match {
      case Some((left, right)) =>
        val merged = new File(base, "%s-%s.merged".format(left.name, right.name))

        segmentedCompactor.mergeSegments(left, right, merged)
        replaceLock synchronized {
          replaceSegment(left, merged.getAbsolutePath)
          removeSegment(right)
        }
        merge()

      case None => // Nothing to merge
    }
 }

  /**
   * Create a new liveDir, based on the current max dirNum + 1. Move current
   * liveDir into read-only mode.
   */
  private def split() = replaceLock.synchronized {
    readOnlyDirs :+= liveDir
    val newDirNum = readOnlyDirs.map(_.name.toLong).max + 1
    liveDir = Segment(base, newDirNum.toString)
  }

  /**
   * MUTATES READONLYDIRS!
   *
   * For all the Segment -> location mappings provided, will do the following:
   *  - remove existing segment files, replace with the compacted files
   *  - remove the current Segment from readOnlyDirs, replace with compacted Segment
   *
   * @param replacementMap map of existing Segment -> location of compacted segment
   */
  private def replaceSegments(replacementMap: Map[Segment, String]) = replaceLock.synchronized {
    for (segment <- replacementMap.keys) {
      replaceSegment(segment, replacementMap(segment))
    }
  }

  private def replaceSegment(original: Segment, replacement: String) = replaceLock.synchronized {
    val newSegment = segmentedCompactor.replace(original, replacement)
    readOnlyDirs = replaceElement(readOnlyDirs, original, newSegment)
  }

  private def removeSegment(segment: Segment) = replaceLock.synchronized {
    readOnlyDirs = readOnlyDirs.filterNot(_ == segment)
    segmentedCompactor.delete(segment)
  }

  /**
   * Generates a new list, replacing all occurrences of 'remove' found with 'replace'
   * @param list list to be changed
   * @param remove element to remove
   * @param replace element to add
   * @tparam T type of list
   * @return resulting list
   */
  private def replaceElement[T](list: List[T], remove: T, replace: T): List[T] = {
    list.map {
      case elem if elem == remove => replace
      case elem => elem
    }
  }

}
