package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.api.impl.OrderedEvent

/**
 * Defines the Log Writer
 */
trait SiriusLog extends LogIteratorSource {

  /**
   * Write an entry to the log
   *
   * @param OrderedEvent entry the entry to write to the log
   */
  def writeEntry(entry: OrderedEvent)

  /**
   * Fold left across the log entries
   * @param acc0 initial accumulator value
   * @param foldFun function to apply to the log entry, the result being the new accumulator
   */
  def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T

  def getNextSeq: Long

}
