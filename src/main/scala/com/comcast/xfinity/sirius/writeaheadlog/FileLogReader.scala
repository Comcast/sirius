package com.comcast.xfinity.sirius.writeaheadlog

import java.io.File
import scalax.io.Line.Terminators.CarriageReturn
import org.slf4j.LoggerFactory
import scalax.io.{LongTraversable, Resource}

/**
 * Class that reads a log from a file
 */
class FileLogReader(filePath: String, serDe: LogDataSerDe) extends LogReader {
  val logger = LoggerFactory.getLogger(classOf[FileLogReader])

  /**
   * ${@inheritDoc}
   */
  override def readEntries(processEntry: LogData => Unit) = {
    lines.foreach((line) => {
      logger.debug(line)
      val entry = serDe.deserialize(line)
      logger.debug(entry.key)
      processEntry(entry)
    })

  }

  private[writeaheadlog] def lines = {
    Resource.fromFile(new File(filePath)).lines(CarriageReturn)
  }
}