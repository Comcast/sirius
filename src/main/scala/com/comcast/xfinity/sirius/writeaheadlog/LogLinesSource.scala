package com.comcast.xfinity.sirius.writeaheadlog

/**
 * Encapsulate the reading of one or more log files as a single stream of entries
 */
trait LogLinesSource {

  /**
   * Retrieve lines of entire log for sequential reading
   *
   * @return Iterator of lines in one (or more) log file(s)
   */
  def createLinesIterator(): Iterator[String]
}
