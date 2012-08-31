package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.persistence.{BoundedLogRange, LogRange}
import scala.collection.JavaConversions._
import java.util.{TreeMap => JTreeMap}
import scalax.io.CloseableIterator

object CachedSiriusLog {
  /**
   * Create CachedSiriusLog based on log, using the default value
   * for maxCacheSize.
   *
   * @param log siriusLog to use as backend
   */
  def apply(log: SiriusLog): CachedSiriusLog = {
    // XXX should be passed in via SiriusConfiguration
    val MAX_CACHE_SIZE = 10000

    new CachedSiriusLog(log, MAX_CACHE_SIZE)
  }
}

/**
 * Wrapper around any implementation of SiriusLog that enables caching.
 *
 * @param log SiriusLog to use as backend
 * @param maxCacheSize maximum number of events to keep cached
 */
class CachedSiriusLog(log: SiriusLog, maxCacheSize: Int) extends SiriusLog {

  // XXX: partly-private for testing
  private[writeaheadlog] val writeCache = new JTreeMap[Long, OrderedEvent]()
  // keeping track of lowest and highest seq values for less synchronization
  private[writeaheadlog] var firstSeq = -1L
  private[writeaheadlog] var lastSeq = -1L

  /**
   * ${inheritDoc}
   *
   * Writes both to disk and to the in-mem write cache.  Trims cache if necessary,
   * depending on MAX_CACHE_SIZE.
   *
   * Side-effect of updating lastSeq.  THIS ASSUMES THAT WRITES OCCUR IN-ORDER.
   *
   * @param entry OrderedEvent to write
   */
  override def writeEntry(entry: OrderedEvent) {
    writeCache.synchronized {
      log.writeEntry(entry)

      writeCache.put(entry.sequence, entry)
      updateSeq(entry)

      if (writeCache.size() > maxCacheSize) {
        writeCache.remove(writeCache.firstKey())
        firstSeq = writeCache.firstKey
      }
    }
  }

  // set seq counters that we use to avoid synchronizing for flow control
  private def updateSeq(entry: OrderedEvent) {
    if (firstSeq == -1L) {
      firstSeq = entry.sequence
    }
    lastSeq = entry.sequence
  }

  /**
   * ${inheritDoc}
   *
   * For bounded LogRange requests, will check write cache first, replying with
   * entries from memory if possible.  This should avoid seeking the range on disk
   * in most catch-up cases.
   *
   * @param logRange the LogRange for the events to iterate
   * @return Iterator of events in one (or more) log file(s)
   */
  override def createIterator(logRange: LogRange): CloseableIterator[OrderedEvent] = {
    logRange match {
      case BoundedLogRange(start, end) if (firstSeq <= start && end <= lastSeq) =>
        createIteratorCached(start, end)
      case range: LogRange =>
        log.createIterator(logRange)
    }
  }

  private def createIteratorCached(start: Long, end: Long) = writeCache.synchronized {
    CloseableIterator(writeCache.subMap(start, true, end, true).valuesIterator)
  }

  /**
   * Fold left across the log entries
   * @param acc0 initial accumulator value
   * @param foldFun function to apply to the log entry, the result being the new accumulator
   */
  def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T) = log.foldLeft(acc0)(foldFun)

  def getNextSeq = log.getNextSeq
}
