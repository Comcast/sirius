package com.comcast.xfinity.sirius.writeaheadlog

/**
 * Defines a way to read from a log
 */
trait LogReader  {

  /**
   * Reads the entries from the log, converts it to a LogData, and calls processEntry with the results
   *
   * @param (LogData => Unit) processEntry The callback that processes each log entry
   */
  def readEntries(processEntry: LogData => Unit)

  /**
   * Fold left across the log entries
   * @param acc0 initial accumulator value
   * @param foldFun function to apply to the log entry, the result being the new accumulator
   */
  def foldLeft[T](acc0: T)(foldFun: (T, LogData) => T): T
  
}
