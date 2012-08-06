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
    new SiriusFileLog(logPath, new WriteAheadLogSerDe(), new WalFileOps())
}

/**
 * An append only log for storing OrderedEvents.
 *
 * @param logPath location of the log file, it will be created if
 *            it does not exist
 * @param serDe WALSerDe used to serialize events for writing to disk,
 *            generally you should not have to use this parameter, use
 *            the companion object constructor instead
 * @param walFileOps WalFileOps used for miscellaneous low level file
 *            operations.  Generally you should not have to use this
 *            parameter, use the companion object constructor instead
 */
class SiriusFileLog(logPath: String,
                    serDe: WALSerDe,
                    walFileOps: WalFileOps) extends SiriusLog {

  /**
   * This constructor is here for Java compatibility because we
   * currently expect end users to pass this in, in the future
   * we will want to have the log constructed within sirius, and
   * we will no longer need this constructor
   */
  def this(logPath: String, serDe: WALSerDe) =
    this(logPath, serDe, new WalFileOps)

  // XXX: this is a normal logger, but will this class will reside almost
  //      exclusively within an actor system, logging should be minimal
  private val logger = LoggerFactory.getLogger(classOf[SiriusFileLog])

  // XXX: needs to be lazy for testing
  lazy val file: Path = Path.fromString(logPath)

  private var nextPossibleSeq: Long = walFileOps.getLastLine(logPath) match {
    case None => 1
    case Some(line) =>
      val seq = serDe.deserialize(line).sequence
      seq + 1
  }

  logger.info("Starting SiriusFileLog with next sequence " +
    nextPossibleSeq)

  /**
   * ${@inheritDoc}
   *
   * Will only write in-order events to log.  Attempts to write events with sequence numbers
   * less than the current sequence number will result in an IllegalArgumentException.
   */
  override def writeEntry(entry: OrderedEvent) {
    if (entry.sequence >= nextPossibleSeq) {
      val rawData: String = serDe.serialize(entry)
      file.append(rawData)
      nextPossibleSeq = entry.sequence + 1

      logger.debug("Wrote event to file " + logPath + ":  " + rawData)
    } else {
      throw new IllegalArgumentException("Attempted to write out-of-order event: expected >= " +
        nextPossibleSeq + ", got " + entry.sequence)
    }
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

  override def getNextSeq = nextPossibleSeq
}
