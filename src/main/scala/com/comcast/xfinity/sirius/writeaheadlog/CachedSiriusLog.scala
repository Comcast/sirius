/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import scala.collection.JavaConversions._
import java.util.{TreeMap => JTreeMap}
import com.comcast.xfinity.sirius.api.SiriusConfiguration

object CachedSiriusLog {
  /**
   * Create CachedSiriusLog based on log, using the default value
   * for maxCacheSize.
   *
   * @param log siriusLog to use as backend
   */
  def apply(log: SiriusLog, cacheSize: Int): CachedSiriusLog =
    new CachedSiriusLog(log, cacheSize)
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
   * @inheritdoc
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
   * @inheritdoc
   *
   * For bounded LogRange requests, will check write cache first, replying with
   * entries from memory if possible.  This should avoid seeking the range on disk
   * in most catch-up cases.
   *
   * @param startSeq sequence number to start folding over, inclusive
   * @param endSeq sequence number to end folding over, inclusive
   * @param acc0 the starting value of the accumulator
   * @param foldFun function that should take (accumulator, event) and return a new accumulator
   * @return the final value of the accumulator
   */
  override def foldLeftRange[T](startSeq: Long, endSeq: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T = {
    (startSeq, endSeq) match {
      case (start, end) if (firstSeq <= start && end <= lastSeq) =>
        foldLeftRangeCached(start, end)(acc0)(foldFun)
      case (start, end) =>
        log.foldLeftRange(start, end)(acc0)(foldFun)
    }
  }

  /**
   * Private inner version of fold left.  This one hits the cache, and assumes that start/endSeqs are
   * contained in the cache.  Synchronizes on writeCache so we can subMap with no fear.
   */
  private def foldLeftRangeCached[T](startSeq: Long, endSeq: Long)
                                     (acc0: T)(foldFun: (T, OrderedEvent) => T): T = writeCache.synchronized {
    writeCache.subMap(startSeq, true, endSeq, true).values.foldLeft(acc0)(foldFun)
  }

  def getNextSeq = log.getNextSeq

  def compact() {
    log.compact()
  }

  def size() : Long = {
    log.size
  }
}
