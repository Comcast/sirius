package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import org.slf4j.LoggerFactory
import scalax.io.CloseableIterator
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.persistence.LogRange

class DoNothingSiriusLog extends SiriusLog {
  private val logger = LoggerFactory.getLogger(classOf[DoNothingSiriusLog])

  override def writeEntry(entry: OrderedEvent): Boolean =  {
    logger.info("Writing entry for {}", entry)
    true
  }

  override def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T = acc0

  override def createIterator(logRange: LogRange): CloseableIterator[OrderedEvent] =
    CloseableIterator(Iterator[OrderedEvent]())
}