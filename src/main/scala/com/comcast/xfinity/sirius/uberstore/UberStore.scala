package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.uberstore.segmented.SegmentedUberStore
import java.io.File

object UberStore {

  /**
   * Create an UberStore based in baseDir.  baseDir is NOT
   * created, it must exist. The files within baseDir will
   * be created if they do not exist however.
   *
   * @param baseDir directory to base the UberStore in
   *
   * @return an instantiated UberStore
   */
  def apply(baseDir: String): UberStore = {
    if (!isValidUberStore(baseDir)) {
      throw new IllegalStateException("Cannot start. Configured to boot with legacy UberStore, but other UberStore format found.")
    }

    new UberStore(UberPair(baseDir, 1L))
  }

  /**
   * There's no explicit marker for Legacy UberStores. All we can do is check that it's not any of
   * the other known varieties.
   *
   * @param baseDir directory UberStore is based in
   * @return true if baseDir corresponds to a valid legacy UberStore, false otherwise
   */
  def isValidUberStore(baseDir: String): Boolean = {
    !UberTool.isSegmented(baseDir)
  }
}

/**
 * Expectedly high performance sequence number based append only
 * storage.  Stores all data in dataFile, and sequence -> data
 * mappings in index.
 *
 * @param uberpair UberStoreFilePair for delegating uberstore operations
 */
class UberStore private[uberstore] (uberpair: UberPair) extends SiriusLog {

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

  def compact() {
    // do nothing, don't use compact() on legacy uberstore
  }
}
