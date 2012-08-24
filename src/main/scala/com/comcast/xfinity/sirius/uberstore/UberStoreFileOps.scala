package com.comcast.xfinity.sirius.uberstore

import java.io.RandomAccessFile

/**
 * Trait providing low level File operations for an UberStore
 */
trait UberStoreFileOps {

  /**
   * Write body into writeHandle using standard entry encoding
   * procedures.
   *
   * Has the side effect of advancing writeHandle to the end
   * of the written data
   *
   * @param writeHandle the file to write the body into
   * @param body the body of what we want stored
   *
   * @return offset this object was stored at
   */
  def put(writeHandle: RandomAccessFile, body: Array[Byte]): Long

  /**
   * Read the next entry out of the file, from the current offset
   *
   * Has the side effect of advancing readHandle to the end of
   * the written data
   *
   * @param readHandle the RandomAccessFile to read from, at the
   *          current offset
   *
   * @return Some(bytes) or None if EOF encountered
   */
  def readNext(readHandle: RandomAccessFile): Option[Array[Byte]]
}