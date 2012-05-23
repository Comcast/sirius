package com.comcast.xfinity.sirius.writeaheadlog

/**
 * Defines the Log Writer
 */
trait LogWriter {

  /**
   * Write an entry to the log
   *
   * @param LogData entry the entry to write to the log
   */
  def writeEntry(entry: LogData)

}
