package com.comcast.xfinity.sirius.writeaheadlog

/**
 * Defines a way to read from a log
 */
trait LogReader {


  /**
   * Fold left across the log entries
   * @param acc0 initial accumulator value
   * @param foldFun function to apply to the log entry, the result being the new accumulator
   */
  def foldLeft[T](acc0: T)(foldFun: (T, LogData) => T): T

}
