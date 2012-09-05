package com.comcast.xfinity.sirius.uberstore.seqindex

import java.io.RandomAccessFile
import com.comcast.xfinity.sirius.uberstore.Fnv1aChecksummer
import java.util.{TreeMap => JTreeMap}

object ReadOnlySeqIndex {
  /**
   * Create a ReadOnlySeqIndex object using seqFileName as the backing file.
   *
   * @param seqFileName the file to use, will be created if it does not
   *          exist
   *
   * @return a ReadOnlySeqIndex instance backed by seqFileName
   */
  def apply(seqFileName: String) = {
    val fileOps = new SeqIndexBinaryFileOps with Fnv1aChecksummer
    val readHandle = new RandomAccessFile(seqFileName, "r")
    new ReadOnlySeqIndex(readHandle, fileOps)
  }
}

/**
 * Slightly tricksy extension of PersistedSeqIndex.  Uses the parent implementations for most of the
 * work, but doesn't accept writes and before each read, checks for updates to the persisted index
 * and applies them.  Since the fileOps.loadIndex uses an open RAF handle and reads from it until
 * the end, we can also use that as a "get what's been added since my last load" operation.  Since
 * this is implementation-dependent, specifying SeqIndexBinaryFileOps (instead of generic SeqIndexFileOps).
 *
 * @param readHandle RandomAccessFile for reading index data
 * @param fileOps Service class passed in for persisting events to some
 *                durable medium
 */
private[seqindex] class ReadOnlySeqIndex(readHandle: RandomAccessFile, fileOps: SeqIndexBinaryFileOps)
  extends PersistedSeqIndex(readHandle, fileOps) {

  /**
   * Refreshes the index first.
   *
   * ${inheritdoc}
   */
  override def getOffsetFor(seq: Long): Option[Long] = synchronized {
    seqCache.putAll(fileOps.loadIndex(readHandle))
    super.getOffsetFor(seq)
  }

  /**
   * Refreshes the index first.
   *
   * ${inheritdoc}
   */
  override def getMaxSeq: Option[Long] = synchronized {
    seqCache.putAll(fileOps.loadIndex(readHandle))
    super.getMaxSeq
  }

  /**
   * Refreshes the index first.
   *
   * ${inheritdoc}
   */
  override def getOffsetRange(firstSeq: Long, lastSeq: Long): (Long,  Long) = synchronized {
    seqCache.putAll(fileOps.loadIndex(readHandle))
    super.getOffsetRange(firstSeq, lastSeq)
  }

  /**
   * Throw UnsupportedOperationException: cannot put() to ReadOnlySeqIndex
   * @param seq sequence number
   * @param offset offset
   */
  override def put(seq: Long, offset: Long) {
    throw new UnsupportedOperationException("Attempting to put() on a read-only SeqIndex")
  }

  override def close() {
    // no op
  }
}
