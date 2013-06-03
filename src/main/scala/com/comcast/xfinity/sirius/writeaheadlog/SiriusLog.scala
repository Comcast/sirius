package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.api.impl.OrderedEvent

/**
 * Defines the Log Writer
 */
trait SiriusLog {

  /**
   * Write an entry to the log
   *
   * @param entry the OrderedEvent to write to the log
   */
  def writeEntry(entry: OrderedEvent)

  /**
   * Fold left across the log entries
   * @param acc0 initial accumulator value
   * @param foldFun function to apply to the log entry, the result being the new accumulator
   */
  def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T = foldLeftRange(0, Long.MaxValue)(acc0)(foldFun)

  /**
   * Fold left across a specified range of log entries
   * @param startSeq sequence number to start with, inclusive
   * @param endSeq sequence number to end with, inclusive
   * @param acc0 initial accumulator value
   * @param foldFun function to apply to the log entry, the result being the new accumulator
   */
  def foldLeftRange[T](startSeq: Long, endSeq: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T

  def getNextSeq: Long

}
