package com.comcast.xfinity.sirius.uberstore.seqindex

import java.io.RandomAccessFile
import java.util.{TreeMap => JTreeMap}
import com.comcast.xfinity.sirius.uberstore.common.Fnv1aChecksummer

object PersistedSeqIndex {

  /**
   * Create a PersistedSeqIndex object using seqFileName as the backing file.
   *
   * @param seqFileName the file to use, will be created if it does not
   *          exist
   *
   * @return a SeqIndex instance backed by seqFileName
   */
  def apply(seqFileName: String) = {
    val fileOps = SeqIndexBinaryFileOps()
    val writeHandle = new RandomAccessFile(seqFileName, "rw")
    new PersistedSeqIndex(writeHandle, fileOps)
  }
}

/**
 * PRIVATE CONSTRUCTOR
 *
 * Create a PersistedSeqIndex object, which is a lower level construct of the UberLog.
 * All read/write operations take place on the passed in file handle.
 *
 * Instances constructed with the same handle instance are not thread
 * safe with respect to each other for instantiation and put operations.
 *
 * @param writeHandle The RandomAccessFile to use for persisting to
 *          disk, as well as populating this index.
 *          NOTE: the passed in handle should be at offset 0
 * @param fileOps Service class passed in for persisting events to some
 *          durable medium
 */
// TODO: we may be able to just use a standard output stream for this...
private[uberstore] class PersistedSeqIndex(writeHandle: RandomAccessFile,
                                               fileOps: SeqIndexBinaryFileOps) extends SeqIndex {

  val seqCache: JTreeMap[Long, Long] = fileOps.loadIndex(writeHandle)
  var isClosed = false

  /**
   * ${inheritdoc}
   */
  def getOffsetFor(seq: Long): Option[Long] = synchronized {
    if (seqCache.containsKey(seq)) {
      Some(seqCache.get(seq))
    } else {
      None
    }
  }

  /**
   * ${inheritdoc}
   */
  def getMaxSeq: Option[Long] = synchronized {
    if (seqCache.isEmpty) {
      None
    } else {
      Some(seqCache.lastKey())
    }
  }

  /**
   * ${inheritdoc}
   */
  def put(seq: Long, offset: Long) = synchronized {
    if (isClosed) {
      throw new IllegalStateException("Attempting to write to closed SeqIndex")
    }

    fileOps.put(writeHandle, seq, offset)
    seqCache.put(seq, offset)
  }

  /**
   * ${inheritdoc}
   */
  def getOffsetRange(firstSeq: Long, lastSeq: Long): (Long,  Long) = synchronized {
    val range = seqCache.subMap(firstSeq, true, lastSeq, true)

    if (range.isEmpty) {
      (0, -1)
    } else {
      val firstOffset = range.firstEntry.getValue
      val lastOffset = range.lastEntry.getValue
      (firstOffset, lastOffset)
    }
  }

  /**
   * ${inheritdoc}
   */
  def close() {
    if (!isClosed) {
      writeHandle.close()
      isClosed = true
    }
  }

  override def finalize() {
    close()
  }
}

