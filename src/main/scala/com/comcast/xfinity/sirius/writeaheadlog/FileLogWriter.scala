package com.comcast.xfinity.sirius.writeaheadlog

import scalax.file.Path
import org.slf4j.LoggerFactory

/**
 * Class that writes a log to a file
 */
class FileLogWriter(logPath: String, serDe: LogDataSerDe) extends LogWriter {

  private val logger = LoggerFactory.getLogger(classOf[FileLogWriter])


  /**
   * ${@inheritDoc}
   */
  override def writeEntry(entry: LogData) {

    val rawData: String = serDe.serialize(entry)
    file.append(rawData)
    logger.debug("Wrote logData to file " + logPath + ":  " + rawData)
  }

  private[writeaheadlog] val file = {
    Path.fromString(logPath)
  }
}
