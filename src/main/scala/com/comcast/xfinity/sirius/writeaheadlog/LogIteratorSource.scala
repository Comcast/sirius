package com.comcast.xfinity.sirius.writeaheadlog

import scalax.io.CloseableIterator
import com.comcast.xfinity.sirius.api.impl.persistence.LogRange
import com.comcast.xfinity.sirius.api.impl.OrderedEvent

/**
 * Encapsulate the reading of one or more log files as a single stream of entries
 */
trait LogIteratorSource {

  /**
   * Retrieve a specified range of events from the log for sequential reading
   *
   * @param logRange the LogRange for the events to iterate
   * @return Iterator of events in one (or more) log file(s)
   */
  def createIterator(logRange: LogRange): CloseableIterator[OrderedEvent]
}
