package com.comcast.xfinity.sirius.uberstore.segmented

import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import java.io.File
import com.comcast.xfinity.sirius.api.impl.OrderedEvent

object SegmentedUberStore {

  /**
   * Create an SegmentedUberStore based in base.  base is NOT
   * created, it must exist. The files within base will
   * be created if they do not exist however.
   *
   * @param base directory to base the SegmentedUberStore in
   *
   * @return an instantiated SegmentedUberStore
   */
  def apply(base: String): SegmentedUberStore = {
    // TODO make configurable
    val MAX_EVENTS_PER_SEGMENT = 4000000L

    new SegmentedUberStore(new File(base), MAX_EVENTS_PER_SEGMENT)
  }
}

/**
 * Expectedly high performance sequence number based append only
 * storage.
 *
 * @param base directory SegmentedUberStore is based in
 */
class SegmentedUberStore private[segmented] (base: File, eventsPerSegment: Long) extends SiriusLog {

  val replaceLock = new Object()
  val compactLock = new Object()

  // XXX incorporate maxDeleteAge
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
   * For each unapplied Segment, compact previous segments according to its keys.
   *
   * Continues as long as there are unapplied segments.
   */
  def compact() = compactLock.synchronized {
    for (toCompact <- SegmentedCompactor.findCompactableSegments(readOnlyDirs)) {
      val compactionMap = SegmentedCompactor.compactAgainst(toCompact, readOnlyDirs)
      replaceSegments(compactionMap) // mutates readOnlyDirs
      toCompact.setApplied(applied = true)
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
      val newSegment = SegmentedCompactor.replace(segment, replacementMap(segment))
      readOnlyDirs = replaceElement(readOnlyDirs, segment, newSegment)
    }
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
