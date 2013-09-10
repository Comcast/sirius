package com.comcast.xfinity.sirius.uberstore.segmented

import scalax.file.Path

object SegmentedCompactor {

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

    originalPath.deleteRecursively()
    replacementPath.moveTo(originalPath)

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
   * @param toCompact segment to compact against
   * @param allSegments list of segments to be compacted
   * @return Map of (old Segment -> compacted Segment location)
   */
  def compactAgainst(toCompact: Segment, allSegments: List[Segment]): Map[Segment, String] =
    compactSegments(toCompact, allSegments.filter(_.name.toLong < toCompact.name.toLong))

  /**
   * Compact all provided Segments against the provided Segment. There is no filtering in this method: only
   * provide the Segments you want compacted.
   *
   * @param toCompact Segment to compact against
   * @param segments segments to be compacted
   */
  private def compactSegments(toCompact: Segment, segments: List[Segment]): Map[Segment, String] = {
    val keys = toCompact.keys
    segments.foldLeft(Map[Segment, String]())(
      (map, toCompact) => {
        val compactInto = Segment(toCompact.location.getParentFile, toCompact.name + ".compacting")
        compactSegment(keys, toCompact, compactInto)

        compactInto.setApplied(toCompact.isApplied)
        compactInto.close()

        map + (toCompact -> compactInto.location.getAbsolutePath)
      })
  }

  /**
   * Compact a single Segment, according to the GC keys provided
   *
   * @param keys keys to gc against
   * @param source source segment, to be GC'ed
   * @param dest destination segment, empty, will be populated
   */
  private def compactSegment(keys: Set[String], source: Segment, dest: Segment) {
    // XXX if we want to remove old deletes from the log...
    // val currentTime = System.currentTimeMillis
    source.foreach {
      // case OrderedEvent(_, timestamp, Delete(key)) if (currentTime - timestamp) > MAX_DELETE_AGE => // nothing
      case event if !keys.contains(event.request.key) => dest.writeEntry(event)
      case _ => // this event is overridden by some future event: garbage collect it.
    }
  }
}
