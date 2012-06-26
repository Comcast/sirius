package com.comcast.xfinity.sirius.writeaheadlog

import scalax.file.Path
import org.slf4j.LoggerFactory
import scalax.io.Line.Terminators.NewLine

/**
 * Class that writes a log to a file
 */
class SiriusFileLog(logPath: String, serDe: LogDataSerDe) extends SiriusLog {
  private val logger = LoggerFactory.getLogger(classOf[SiriusFileLog])

  private[writeaheadlog] val file = Path.fromString(logPath)

  /**
   * ${@inheritDoc}
   */
  override def writeEntry(entry: LogData) {
    val rawData: String = serDe.serialize(entry)
    file.append(rawData)
    logger.debug("Wrote logData to file " + logPath + ":  " + rawData)
  }

  /**
   * ${@inheritDoc}
   */
  override def foldLeft[T](acc0: T)(foldFun: (T, LogData) => T): T = {
    val lines = file.lines(NewLine, true)
    lines.foldLeft(acc0)((acc: T, line: String) => foldFun(acc, serDe.deserialize(line)))
  }
}
