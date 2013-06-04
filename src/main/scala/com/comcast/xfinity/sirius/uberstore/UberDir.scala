package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.uberstore.seqindex.DiskOnlySeqIndex
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import data.UberDataFile
import seqindex.SeqIndex
import java.io.File

object UberDir {

  /**
   * Create an UberDir based in baseDir named "name".
   *
   * @param baseDir directory containing the UberDir
   * @param name the name of this dir
   *
   * @return an UberDir instance, fully repaired and usable
   */
  // XXX: developer's note: may want to just combine basedir and name,
  //      so far it looks like we may not need it...
  def apply(baseDir: String, name: String): UberDir = {
    val dir = new File("%s/%s".format(baseDir, name))
    if (!dir.exists()) {
      dir.mkdir()
    }

    val dataFile = new File(dir, "data")
    val indexFile = new File(dir, "index")

    val uberDataFile = UberDataFile(dataFile.getAbsolutePath)
    val index = DiskOnlySeqIndex(indexFile.getAbsolutePath)
    repairIndex(index, uberDataFile)
    new UberDir(uberDataFile, index)
  }

  /**
   * Recovers missing entries at the end of index from dataFile.
   *
   * Assumes that UberDataFile is proper, that is events are there in order,
   * and there are no dups.
   *
   * Has the side effect of updating index.
   *
   * @param index the SeqIndex to update
   * @param dataFile the UberDataFile to update
   */
  private[uberstore] def repairIndex(index: SeqIndex, dataFile: UberDataFile) {
    val (includeFirst, lastOffset) = index.getMaxSeq match {
      case None => (true, 0L)
      case Some(seq) => (false, index.getOffsetFor(seq).get) // has to exist
    }

    dataFile.foldLeftRange(lastOffset, Long.MaxValue)(includeFirst) (
      (shouldInclude, off, evt) => {
        if (shouldInclude) {
          index.put(evt.sequence, off)
        }
        true
      }
    )

  }
}

/**
 * Expectedly high performance sequence number based append only
 * storage directory.  Stores all data in dataFile, and sequence -> data
 * mappings in index.
 *
 * @param dataFile the UberDataFile to store data in
 * @param index the SeqIndex to use
 */
class UberDir private[uberstore](dataFile: UberDataFile, index: SeqIndex) {

  /**
   * Write OrderedEvent event into this dir. Will fail if closed or sequence
   * is out of order.
   *
   * @param event the {@link OrderedEvent} to write
   */
  def writeEntry(event: OrderedEvent) {
    if (isClosed) {
      throw new IllegalStateException("Attempting to write to closed UberDir")
    }
    if (event.sequence < getNextSeq) {
      throw new IllegalArgumentException("Writing events out of order is bad news bears")
    }
    val offset = dataFile.writeEvent(event)
    index.put(event.sequence, offset)
  }

  /**
   * Get the next possible sequence number in this dir.
   */
  def getNextSeq = index.getMaxSeq match {
    case None => 1L
    case Some(seq) => seq + 1
  }

  /**
   * Fold left over all entries.
   *
   * @param acc0 initial accumulator value
   * @param foldFun the fold function
   */
  def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T =
    foldLeftRange(0, Long.MaxValue)(acc0)(foldFun)

  /**
   * Fold left over a range of entries based on sequence number.
   */
  def foldLeftRange[T](startSeq: Long, endSeq: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T = {
    val (startOffset, endOffset) = index.getOffsetRange(startSeq, endSeq)
    dataFile.foldLeftRange(startOffset, endOffset)(acc0)(
      (acc, _, evt) => foldFun(acc, evt)
    )
  }

  /**
   * Close underlying file handles or connections.  This UberDir should not be used after
   * close is called.
   */
  def close() {
    if (!dataFile.isClosed) {
      dataFile.close()
    }
    if (!index.isClosed) {
      index.close()
    }
  }

  /**
   * Consider this closed if either of its underlying objects are closed, no good writes
   * will be able to go through in that case.
   *
   * @return whether this is "closed," i.e., unable to be written to
   */
  def isClosed = dataFile.isClosed || index.isClosed

}
