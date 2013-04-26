package com.comcast.xfinity.sirius.uberstore.seqindex

import java.io.RandomAccessFile
import com.comcast.xfinity.sirius.uberstore.common.Fnv1aChecksummer
import scala.annotation.tailrec

object DiskOnlySeqIndex {

  /**
   * Create an instance from sequence file name
   *
   * @param seqIndexFileName the file name to use
   */
  def apply(seqIndexFileName: String) = {
    val handle = new RandomAccessFile(seqIndexFileName, "rw")
    new DiskOnlySeqIndex(handle, SeqIndexBinaryFileOps())
  }
}

/**
 * UberStore SeqIndex implementation that has next to no memory overhead
 * at the expense of relying heavily on disk operations.  This implementation
 * should play well with the file system cache, which probably does a better
 * job than anything we could put together at the JVM level, so disk operations
 * shouldn't be too terrible.
 *
 * Offset lookups are done using an on disk binary search.
 *
 * THIS CLASS IS NOT THREAD SAFE, nothing is synchronized for now, but this may
 * change if it turns out we don't know how to behave.
 *
 * @param handle the RandomAccessFile associated with the index file.
 * @param fileOps the SeqIndexBinaryFileOps to use when accessing the index file,
 *          DiskOnlySeqIndex does all disk access through this helper
 */
class DiskOnlySeqIndex private(val handle: RandomAccessFile,
                               val fileOps: SeqIndexBinaryFileOps) extends SeqIndex {

  // via some scala magic this is also exposed as a method :)
  var isClosed = false

  /**
   * {@inheritdoc}
   */
  def getOffsetFor(soughtSeq: Long): Option[Long] = synchronized {
    // binary search seq index for what we want
    @tailrec
    def getOffsetForAux(begin: Long, end: Long): Option[Long] = {
      if (begin >= end) {
        None
      } else {
        val mid = ((begin + end) / 24 / 2) * 24
        handle.seek(mid)
        val (seq, offset) = fileOps.readEntry(handle)
        if (seq == soughtSeq) {
          Some(offset)
        } else if (soughtSeq < seq) {
          getOffsetForAux(begin, mid)
        } else {
          getOffsetForAux(mid + 24, end)
        }
      }
    }

    getOffsetForAux(0, handle.length)
  }

  /**
   * {@inheritdoc}
   */
  def getMaxSeq(): Option[Long] = {
    if (handle.length == 0) None
    else {
      handle.seek(handle.length - 24)
      val (seq, _) = fileOps.readEntry(handle)
      Some(seq)
    }
  }

  /**
   * {@inheritdoc}
   */
  def put(seq: Long, offset: Long): Unit = {
    handle.seek(handle.length)
    fileOps.put(handle, seq, offset)
  }

  /**
   * {@inheritdoc}
   */
  def getOffsetRange(firstSeq: Long, lastSeq: Long): (Long, Long) = {
    val rangeOpt = for (
        lowerBound <- getLowerBoundOffset(firstSeq);
        upperBound <- getUpperBoundOffset(lastSeq);
        if lowerBound <= upperBound
      ) yield (lowerBound, upperBound)
    rangeOpt.getOrElse((0, -1))
  }

  /**
   * {@inheritdoc}
   */
  def close() {
    if (!isClosed) {
      handle.close()
      isClosed = true
    }
  }

  private def getLowerBoundOffset(soughtSeq: Long): Option[Long] = {
    @tailrec
    def getLowerBoundOffsetAux(begin: Long, end: Long, closest: Option[Long]): Option[Long] = {
      if (begin >= end) {
        closest
      } else {
        val mid = ((begin + end) / 24 / 2) * 24
        handle.seek(mid)
        val (seq, offset) = fileOps.readEntry(handle)
        if (seq == soughtSeq) {
          Some(offset)
        } else if (soughtSeq < seq) {
          getLowerBoundOffsetAux(begin, mid, Some(offset))
        } else {
          getLowerBoundOffsetAux(mid + 24, end, closest)
        }
      }
    }

    getLowerBoundOffsetAux(0, handle.length, None)
  }

  private def getUpperBoundOffset(soughtSeq: Long): Option[Long] = {
    @tailrec
    def getLowerBoundOffsetAux(begin: Long, end: Long, closest: Option[Long]): Option[Long] = {
      if (begin >= end) {
        closest
      } else {
        val mid = ((begin + end) / 24 / 2) * 24
        handle.seek(mid)
        val (seq, offset) = fileOps.readEntry(handle)
        if (seq == soughtSeq) {
          Some(offset)
        } else if (soughtSeq < seq) {
          getLowerBoundOffsetAux(begin, mid, closest)
        } else {
          getLowerBoundOffsetAux(mid + 24, end, Some(offset))
        }
      }
    }

    getLowerBoundOffsetAux(0, handle.length, None)
  }

}