package com.comcast.xfinity.sirius.uberstore.seqindex

import java.io.RandomAccessFile
import com.comcast.xfinity.sirius.uberstore.Fnv1aChecksummer
import java.util.{TreeMap => JTreeMap}

object SeqIndex {

  /**
   * Create a SeqIndex object using seqFileName as the backing file.
   *
   * @param seqFileName the file to use, will be created if it does not
   *          exist
   *
   * @return a SeqIndex instance backed by seqFileName
   */
  def apply(seqFileName: String) = {
    val fileOps = new SeqIndexBinaryFileOps with Fnv1aChecksummer
    val writeHandle = new RandomAccessFile(seqFileName, "rw")
    new SeqIndex(writeHandle, fileOps)
  }
}

/**
 * PRIVATE CONSTRUCTOR
 *
 * Create a SeqIndex object, which is a lower level construct of the UberLog.
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
// TODO: use a trait to hide this?
private[uberstore] class SeqIndex(writeHandle: RandomAccessFile, fileOps: SeqIndexFileOps) {

  val seqCache: JTreeMap[Long, Long] = fileOps.loadIndex(writeHandle)
  var isClosed = false

  /**
   * Get the offset for a particular sequence number
   *
   * @param seq Sequence number to find offset for
   *
   * @return Some(offset) if found, None if not
   */
  def getOffsetFor(seq: Long): Option[Long] = {
    if (seqCache.containsKey(seq)) {
      Some(seqCache.get(seq))
    } else {
      None
    }
  }

  /**
   * Get the maximum sequence number stored, if such exists
   *
   * @return Some(sequence) or None if none such exists
   */
  def getMaxSeq: Option[Long] =
    if (seqCache.isEmpty) {
      None
    } else {
      Some(seqCache.lastKey())
    }

  /**
   * Map seq -> offset, persisting to disk and memory
   *
   * This operation is not thread safe relative to other
   * put operations.
   *
   * Subsequent Seq/Offset pairs should be strictly increasing,
   * for now behavior is undefined if they are not, in the future
   * we may enforce this more vigorously.ugh
   *
   * @param seq sequence number
   * @param offset offset
   */
  def put(seq: Long, offset: Long) {
    if (isClosed) {
      throw new IllegalStateException("Attempting to write to closed SeqIndex")
    }

    fileOps.put(writeHandle, seq, offset)
    seqCache.put(seq, offset)
  }

  /**
   * Get the range of offsets for entries for sequence numbers between
   * firstSeq and lastSeq, inclusive.
   *
   * @param firstSeq first sequence number to look for
   * @param lastSeq last sequence number to look for
   *
   * @return Tuple2[Long, Long] with the first item being the offset of
   *          the first entry withing.
   *          If the range is empty, (0, -1) is returned
   */
  def getOffsetRange(firstSeq: Long, lastSeq: Long): (Long,  Long) = {
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
   * Close open file handles. This SeqIndex should not be used after
   * close is called.
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