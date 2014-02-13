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
 * @param handle the RandomAccessFile associated with the index file.
 * @param fileOps the SeqIndexBinaryFileOps to use when accessing the index file,
 *          DiskOnlySeqIndex does all disk access through this helper
 */
class DiskOnlySeqIndex private(handle: RandomAccessFile,
                               fileOps: SeqIndexBinaryFileOps) extends SeqIndex {

  // via some scala magic this is also exposed as a method :)
  var isClosed = false
  var size: Long = handle.length() / 24

  var maxSeq = {
    if (handle.length == 0)
      None
    else {
      handle.seek(handle.length - 24)
      val (seq, _) = fileOps.readEntry(handle)
      Some(seq)
    }
  }

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
  def getMaxSeq(): Option[Long] = maxSeq

  /**
   * {@inheritdoc}
   */
  def put(seq: Long, offset: Long): Unit = synchronized {
    handle.seek(handle.length)
    fileOps.put(handle, seq, offset)
    maxSeq = Some(seq)
    size += 1
  }

  /**
   * {@inheritdoc}
   */
  def getOffsetRange(firstSeq: Long, lastSeq: Long): (Long, Long) = synchronized {
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
  def close(): Unit = synchronized {
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
