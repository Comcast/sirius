package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.writeaheadlog.{LogData, LogWriter}
import org.slf4j.LoggerFactory

class DoNothingLogWriter extends LogWriter {
  private val logger = LoggerFactory.getLogger(classOf[DoNothingLogWriter])

  override def writeEntry(entry: LogData) = {
    logger.info("Writing entry {} - {}", entry.actionType, entry.key)
  }

}