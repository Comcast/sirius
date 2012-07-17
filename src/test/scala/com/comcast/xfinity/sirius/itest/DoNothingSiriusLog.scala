package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import org.slf4j.LoggerFactory
import scalax.io.CloseableIterator
import com.comcast.xfinity.sirius.api.impl.OrderedEvent

class DoNothingSiriusLog extends SiriusLog {
  private val logger = LoggerFactory.getLogger(classOf[DoNothingSiriusLog])

  override def writeEntry(entry: OrderedEvent) {
    logger.info("Writing entry for {}", entry)
  }

  override def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T = acc0

  override def createIterator(): CloseableIterator[OrderedEvent] = null

  override def createRangedIterator(startRange: Long,  endRange: Long): CloseableIterator[OrderedEvent] = null
}