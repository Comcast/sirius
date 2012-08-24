package com.comcast.xfinity.sirius.uberstore.seqindex

import java.io.RandomAccessFile
import com.comcast.xfinity.sirius.uberstore.Fnv1aChecksummer

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
    val handle = new RandomAccessFile(seqFileName, "rw")
    val fileOps = new SeqIndexBinaryFileOps with Fnv1aChecksummer
    new SeqIndex(handle, fileOps)
  }
}

/**
 * PRIVATE CONSTRUCTOR
 *
 * Create a SeqIndex object, which is a lower level construct of the UberLog.
 * All read/write operations take place on the passed in file handle.
 *
 * Instances constructed with the same handle instance are not thread
 * safe with respect to eachother for instantiation and put operations.
 *
 * @param handle The RandomAccessFile to use for persisting to
 *          disk, as well as populating this index.
 *          NOTE: the passed in handle should be at offset 0
 * @param fileOps Service class passed in for persisting events to some
 *          durable medium
 */
// TODO: we may be able to just use a standard output stream for this...
private[seqindex] class SeqIndex(handle: RandomAccessFile, fileOps: SeqIndexFileOps) {

  var seqCache = fileOps.loadIndex(handle)

  /**
   * Get the offset for a particular sequence number
   *
   * @param seq Sequence number to find offset for
   *
   * @return Some(offset) if found, None if not
   */
  def getOffsetFor(seq: Long): Option[Long] = seqCache.get(seq)

  /**
   * Get the maximum sequence number stored, if such exists
   *
   * @return Some(sequence) or None if none such exists
   */
  def getMaxSeq: Option[Long] = seqCache.keySet.lastOption

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
    fileOps.put(handle, seq, offset)
    seqCache += (seq -> offset)
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
    // TODO: this is inefficient probably, make better
    val range = seqCache.filter(
      (kv) => (firstSeq <= kv._1 && kv._1 <= lastSeq)
    )
    if (range.isEmpty) {
      (0, -1)
    } else {
      // Because the range is non-empty these both exist
      val firstOffset = range.headOption.get._2
      val lastOffset = range.lastOption.get._2
      (firstOffset, lastOffset)
    }
  }
}