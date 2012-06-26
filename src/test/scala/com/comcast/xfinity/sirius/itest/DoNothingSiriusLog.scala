package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.writeaheadlog.{LogData, SiriusLog}
import org.slf4j.LoggerFactory

class DoNothingSiriusLog extends SiriusLog {
  private val logger = LoggerFactory.getLogger(classOf[DoNothingSiriusLog])

  override def writeEntry(entry: LogData) {
    logger.info("Writing entry {} - {}", entry.actionType, entry.key)
  }

  override def foldLeft[T](acc0: T)(foldFun: (T, LogData) => T): T = acc0
}