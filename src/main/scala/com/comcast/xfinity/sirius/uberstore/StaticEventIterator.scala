package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import scalax.io.CloseableIterator

/**
 * Class for wrapping a List[OrderedEvent] in a CloseableIterator[OrderedEvent].
 *
 * This class is here for backwards compatibility with the old SiriusLog interface,
 * we intend to eliminate it at our soonest conevenience.
 *
 * @param events the List[OrderedEvent] to wrap
 */
class StaticEventIterator(var events: List[OrderedEvent]) extends CloseableIterator[OrderedEvent] {

  /**
   * @inheritdoc
   */
  def hasNext: Boolean = (events != Nil)

  /**
   * @inheritdoc
   */
  def next(): OrderedEvent = events match {
    case Nil => throw new IllegalStateException("Called next on empty iterator")
    case hd :: tl =>
      events = tl
      hd
  }

  /**
   * @inheritdoc
   */
  def doClose(): List[Throwable] = {
    events = Nil
    Nil
  }
}