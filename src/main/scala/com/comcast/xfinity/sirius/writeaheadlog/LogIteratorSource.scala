package com.comcast.xfinity.sirius.writeaheadlog

import scalax.io.CloseableIterator
import com.comcast.xfinity.sirius.api.impl.OrderedEvent

/**
 * Encapsulate the reading of one or more log files as a single stream of entries
 */
trait LogIteratorSource {

  /**
   * Retrieve events of entire log for sequential reading
   *
   * @return Iterator of events in one (or more) log file(s)
   */
  def createIterator(): CloseableIterator[OrderedEvent]

  /**
   * Retrieve events for a range of the log file
   *
   * @return Iterator of events within the specified range from the log file(s)
   */
  def createRangedIterator(startRange: Long, endRange: Long) : CloseableIterator[OrderedEvent]
}
