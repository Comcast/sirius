package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.persistence.{BoundedLogRange, LogRange}
import scalax.io.CloseableIterator
import scala.collection.JavaConversions._
import java.util

object CachedSiriusFileLog {

  /**
   * Create a CachedSiriusFileLog using the logPath and default WALSerDe
   * (WriteAheadLogSerDe)
   *
   * @param logPath the log file to use
   */
  def apply(logPath: String): SiriusFileLog =
    new CachedSiriusFileLog(logPath, new WriteAheadLogSerDe(), new WalFileOps())
}

class CachedSiriusFileLog(logPath: String, serDe: WALSerDe, walFileOps: WalFileOps)
    extends SiriusFileLog(logPath, serDe, walFileOps) {

  // XXX: lazy for testing, should be passed in from above anyway
  lazy val MAX_CACHE_SIZE = 10000
  // XXX: partly-private for testing
  private[writeaheadlog] val writeCache = new util.TreeMap[Long, OrderedEvent]()

  /**
   * ${inheritDoc}
   *
   * Writes both to disk and to the in-mem write cache.  Trims cache if necessary,
   * depending on MAX_CACHE_SIZE.
   *
   * @param entry
   */
  override def writeEntry(entry: OrderedEvent) {
    super.writeEntry(entry)
    writeCache.put(entry.sequence, entry)
    if (writeCache.size() > MAX_CACHE_SIZE) {
      writeCache.remove(writeCache.firstKey())
    }
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
      case BoundedLogRange(start, end) if (writeCache.firstKey() <= start && end <= writeCache.lastKey()) =>
        CloseableIterator(writeCache.subMap(start, true, end, true).valuesIterator)
      case range: LogRange =>
        super.createIterator(logRange)
    }
  }

}
