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

trait SeqIndex {
  /**
   * Get the offset for a particular sequence number
   *
   * @param seq Sequence number to find offset for
   *
   * @return Some(offset) if found, None if not
   */
  def getOffsetFor(seq: Long): Option[Long]

  /**
   * Get the maximum sequence number stored, if such exists
   *
   * @return Some(sequence) or None if none such exists
   */
  def getMaxSeq: Option[Long]

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
  def put(seq: Long, offset: Long)

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
  def getOffsetRange(firstSeq: Long, lastSeq: Long): (Long,  Long)

  /**
   * Returns whether or not index is closed for use. Closed indexes
   * should not be used.
   * @return true if index is closed, false otherwise
   */
  def isClosed: Boolean

  /**
   * Returns the segment size. If the index is empty then the size is 0.
   * @return the size of the segment as a Long
   */
  def size: Long

  /**
   * Close open file handles. This SeqIndex should not be used after
   * close is called.
   */
  def close()
}

