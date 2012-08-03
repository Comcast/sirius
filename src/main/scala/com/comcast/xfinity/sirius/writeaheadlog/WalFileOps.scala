package com.comcast.xfinity.sirius.writeaheadlog

import java.io.RandomAccessFile

/**
 * Service class for raw file operations on SiriusFileLogs
 *
 * NOTE: this would be better in an object, but mocking of objects
 * with scalamock doesn't work with maven last time I checked
 */
class WalFileOps {

  /**
   * Reads the last line from the file specified by fileName.
   * This file must exist.
   *
   * @param fileName the file to read the last line from, must exist
   *
   * @return None if there is no content, or Some(result) if there is
   */
  def getLastLine(fileName: String): Option[String] = {
    val raf = new RandomAccessFile(fileName, "r")
    try {
      if (raf.length() == 0) {
        None
      } else {
        seekBack(raf)

        val lineLength = (raf.length() - raf.getFilePointer).asInstanceOf[Int]
        val buff = new Array[Byte](lineLength)
        raf.readFully(buff)

        Some(new String(buff))
      }
    } finally {
      raf.close()
    }
  }

  // This method has the side effect of seeking raf to the byte
  // after the first newline encountered when reading the file
  // backwards starting from the byte before the last byte.
  // XXX consider this method a stranger, your mother always told
  //     you to ignore strangers and crazys on the el, avoid eye
  //     contact at all costs.  This method is just like that,
  //     never make eye contact.
  private def seekBack(raf: RandomAccessFile) {
    raf.seek(raf.length())

    var break = false
    var newOff = raf.getFilePointer - 2
    while (newOff >= 0 && !break) {
      raf.seek(newOff)
      if (raf.read() == '\n') {
        break = true
      } else {
        newOff = newOff - 1
      }
    }
    if (newOff < 0) {
      raf.seek(0)
    }
  }

}