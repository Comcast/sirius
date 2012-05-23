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

}
