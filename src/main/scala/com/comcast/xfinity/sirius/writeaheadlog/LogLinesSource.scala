package com.comcast.xfinity.sirius.writeaheadlog

import scalax.io.CloseableIterator

/**
 * Encapsulate the reading of one or more log files as a single stream of entries
 */
trait LogLinesSource {

  /**
   * Retrieve lines of entire log for sequential reading
   *
   * @return Iterator of lines in one (or more) log file(s)
   */
  def createLinesIterator(): CloseableIterator[String]
}
