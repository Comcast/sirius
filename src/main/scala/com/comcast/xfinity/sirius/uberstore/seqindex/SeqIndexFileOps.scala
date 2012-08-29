package com.comcast.xfinity.sirius.uberstore.seqindex

import java.io.RandomAccessFile
import java.util.{TreeMap => JTreeMap}

/**
 * Trait encapsulating basic file operations for the sequence
 * index
 */
trait SeqIndexFileOps {
  /**
   * Persist sequence to offset mapping in the index file at the
   * current position of writeHandle.
   *
   * This function has the side effect of advancing writeHandle
   * to the end of the written data.
   *
   * Thread safety is implementation dependent, but highly
   * unlikely.
   *
   * @param writeHandle the RandomAccessFile to persist into
   * @param seq the sequence number to store
   * @param offset the offset associated with seq
   */
  def put(writeHandle: RandomAccessFile, seq: Long, offset: Long)

  /**
   * Load all sequence -> offset mappings from the input file handle.
   *
   * Has the side effect of advancing the file pointer to the end of
   * the file.
   *
   * Thread safety is implementation dependent, but highly
   * unlikely.
   *
   * @param indexFileHandle the file handle to read from
   *
   * @return the SortedMap[Long, Long] of sequence -> offset mappings
   */
  def loadIndex(indexFileHandle: RandomAccessFile): JTreeMap[Long, Long]
}