package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog

object UberStore {

  /**
   * Create an UberStore based in baseDir.  baseDir is NOT
   * created, it must exist. The files within baseDir will
   * be created if they do not exist however.
   *
   * @param baseDir directory to base the UberStore in
   * @param useMemBackedIndex true to use PersistedSeqIndex, false
   *            to use DiskOnlySeqIndex.  The former offers less
   *            disk activity at the expense of higher memory
   *            overhead (roughly 64b per entry), where the latter
   *            offers lower memory overhead (effectively none)
   *            at the expense of more disk activity. In theory
   *            the higher disk activity of DiskOnlySeqIndex should
   *            be negated by the operating system filesystem cache.
   *            Default is true (use PersistedSeqIndex).
   *
   *
   * @return an instantiated UberStore
   */
  def apply(baseDir: String, useMemBackedIndex: Boolean = true): UberStore = {
    new UberStore(UberStoreFilePair(baseDir, 1L, useMemBackedIndex = useMemBackedIndex))
  }
}

/**
 * Expectedly high performance sequence number based append only
 * storage.  Stores all data in dataFile, and sequence -> data
 * mappings in index.
 *
 * @param uberpair UberStoreFilePair for delegating uberstore operations
 */
class UberStore private[uberstore] (uberpair: UberStoreFilePair) extends SiriusLog {

  /**
   * @inheritdoc
   */
  def writeEntry(event: OrderedEvent) {
    uberpair.writeEntry(event)
  }

  /**
   * @inheritdoc
   */
  def getNextSeq = uberpair.getNextSeq

  /**
   * @inheritdoc
   */
  def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T =
    uberpair.foldLeft(acc0)(foldFun)

  /**
   * @inheritdoc
   */
  def foldLeftRange[T](startSeq: Long, endSeq: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T =
    uberpair.foldLeftRange(startSeq, endSeq)(acc0)(foldFun)

  /**
   * Close underlying file handles or connections.  This UberStore should not be used after
   * close is called.
   */
  def close() {
    uberpair.close()
  }

  /**
   * Consider this closed if either of its underlying objects are closed, no good writes
   * will be able to go through in that case.
   *
   * @return whether this is "closed," i.e., unable to be written to
   */
  def isClosed = uberpair.isClosed

  override def finalize() {
    close()
  }
}