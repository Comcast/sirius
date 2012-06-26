package com.comcast.xfinity.sirius.writeaheadlog

/**
 * Defines the Log Writer
 */
trait SiriusLog {

  /**
   * Write an entry to the log
   *
   * @param LogData entry the entry to write to the log
   */
  def writeEntry(entry: LogData)

  /**
   * Fold left across the log entries
   * @param acc0 initial accumulator value
   * @param foldFun function to apply to the log entry, the result being the new accumulator
   */
  def foldLeft[T](acc0: T)(foldFun: (T, LogData) => T): T

}
