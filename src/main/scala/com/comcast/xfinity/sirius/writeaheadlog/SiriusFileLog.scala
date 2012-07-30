package com.comcast.xfinity.sirius.writeaheadlog

import scalax.file.Path
import org.slf4j.LoggerFactory
import scalax.io.Line.Terminators.NewLine
import scalax.io.CloseableIterator
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.persistence.{BoundedLogRange, EntireLog, LogRange}
import java.io.IOException

object SiriusFileLog {

  /**
   * Create a SiriusFileLog using the logPath and default WALSerDe
   * (WriteAheadLogSerDe)
   *
   * @param logPath the log file to use
   */
  def apply(logPath: String): SiriusFileLog =
    new SiriusFileLog(logPath, new WriteAheadLogSerDe())
}

/**
 * An append only log for storing OrderedEvents.
 *
 * @param logPath location of the log file, it will be created if
 *            it does not exist
 * @param serDe WALSerDe used to serialize events for writing to disk,
 *            generally you should not have to use this parameter, use
 *            the companion object constructor instead
 */
class SiriusFileLog(logPath: String, serDe: WALSerDe) extends SiriusLog {

  private val logger = LoggerFactory.getLogger(classOf[SiriusFileLog])

  private[writeaheadlog] val file = Path.fromString(logPath)

  /**
   * ${@inheritDoc}
   */
  override def writeEntry(entry: OrderedEvent) {
    val rawData: String = serDe.serialize(entry)
    file.append(rawData)
    logger.debug("Wrote event to file " + logPath + ":  " + rawData)
  }

  /**
   * Fold left over the events in this log.  The ordering of OrderedEvents
   * is based on location in file and not sequence number.
   *
   * @param acc0 initial accumulator value
   * @param foldFun function taking the current accumulator value and the
   *          next event, and returning the new accumulator
   */
  override def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T = {
    val lines = file.lines(NewLine, true)
    lines.foldLeft(acc0)((acc: T, line: String) => foldFun(acc, serDe.deserialize(line)))
  }

  /**
   * ${@inheritDoc}
   */
  override def createIterator(logRange: LogRange): CloseableIterator[OrderedEvent] = {
    logRange match {
      case boundedLogRange: BoundedLogRange =>
        new RangedSiriusEventIterator(logPath, serDe, boundedLogRange.start, boundedLogRange.end)
      case EntireLog => new CloseableSiriusEventIterator(logPath, serDe)
      case _ => throw new IOException("Unknown LogRange type")
    }
  }
}
