package com.comcast.xfinity.sirius.writeaheadlog

import java.io.{FileReader, BufferedReader}
import scalax.io.CloseableIterator

/**
 * Implementation of CloseableIterator[String] for sirius logs, which uses a BufferedReader in
 * the background to feed an iterator.  Lines returned from next() WILL CONTAIN the next line and
 * the line terminator. This opens a file normally and returns an iterator that points to the
 * beginning of the file.
 * @param filePath path to sirius WAL file
 */
class CloseableSiriusLineIterator(filePath: String) extends CloseableIterator[String] {
  val br: BufferedReader = new BufferedReader(new FileReader(filePath))

  /**
   * @{inheritDoc}
   *
   * Peek at next byte in reader to see if we have more input, then reset.  If the byte read
   * is -1, that means there is no more input.
   */
  override def hasNext = {
    br.mark(1)
    if (br.read() == -1) {
      false
    } else {
      br.reset()
      true
    }
  }

  /**
   * @{inheritDoc}
   *
   * Grab the next line, including the line terminator.
   */
  override def next() =  {
    br.readLine() + "\n"
  }

  /**
   * @{inheritDoc}
   *
   * Close the back-end file reader.
   */
  override def doClose() = {
    try {
      br.close()
      List[Throwable]()
    } catch {
      case throwable: Throwable =>
        List[Throwable](throwable)
    }
  }
}

