package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import java.io.IOException
import annotation.tailrec

/**
 * Extension of CloseableSiriusLineIterator that returns only events within a range of Sirius
 * sequence numbers.
 *
 * This opens a file normally and returns an iterator that points to the first event in the file that
 * is within the specified range of Sirius sequence numbers.

 * @param filePath path to sirius WAL file
 * @param serDe the WALSerDe to use to de-serialize the WAL file
 */
class RangedSiriusEventIterator(filePath: String, serDe: WALSerDe, val startRange: Long, val endRange: Long)
        extends CloseableSiriusEventIterator(filePath, serDe) {

  private var hasMoreEvents: Boolean = true
  private var nextEvent: Option[OrderedEvent] = None

  /**
   * Check if there is a next OrderedEvent within the specified range of sequence values.
   */
  override def hasNext: Boolean = {
    nextEvent match {
      case Some(_) => true
      case None => findNextEvent()
    }
  }

  /**
   * Get the next OrderedEvent that is within the specified range of sequence values.
   */
  override def next(): OrderedEvent = {
    nextEvent match {
      case Some(event) => {
        findNextEvent()
        event
      }
      case None => {
        findNextEvent()
        if (!hasMoreEvents) throw new IOException("Called next() with no more OrderedEvents")
        nextEvent.get
      }
    }
  }

  @tailrec
  private def findNextEvent() : Boolean = {
    if (!hasMoreEvents) false

    if (!super.hasNext) noMoreEvents()
    else {
      nextEvent = Some(super.next())

      nextEvent.get.sequence match {
        case x if (x < startRange) => findNextEvent()
        case x if (startRange to endRange contains x) => true
        case x if (x > endRange) => noMoreEvents()
      }
    }
  }

  private def noMoreEvents() : Boolean = {
    hasMoreEvents = false
    nextEvent = None
    false
  }
}
