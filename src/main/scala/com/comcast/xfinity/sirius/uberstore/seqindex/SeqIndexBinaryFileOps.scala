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
import java.nio.ByteBuffer
import java.util.{TreeMap => JTreeMap}
import com.comcast.xfinity.sirius.uberstore.common.Checksummer
import com.comcast.xfinity.sirius.uberstore.common.Fnv1aChecksummer
import scala.annotation.tailrec

object SeqIndexBinaryFileOps {

  /**
   * Create an instance using bufferSize buffer size for bulk
   * operations (loadIndex).
   */
  def apply() = {
    // XXX: Buffer sizing is not exposed since at this point since as
    // the code stands it hasn't permeated above this layer.
    // Simpler to leave it out of the picture for now.
    new SeqIndexBinaryFileOps(Fnv1aChecksummer())
  }

}

/**
 * Class providing low level file operations for a binary
 * based sequence index with checksum based data protection.
 *
 * @param checksummer Checksummer used to calculate entry checksums
 * @param bufferSize size of the buffer to be used for reading from passed
 *          in handles.  Each read operation will have its own buffer
 */
class SeqIndexBinaryFileOps private[seqindex](checksummer: Checksummer,
                                              bufferSize: Int = 24 * 1024) {

  /**
   * Persist sequence to offset mapping in the index file at the
   * current position of writeHandle.
   *
   * This function has the side effect of advancing writeHandle
   * to the end of the written data.
   *
   * Not thread safe with respect to writeHandle
   *
   * @param writeHandle the RandomAccessFile to persist into
   * @param seq the sequence number to store
   * @param offset the offset associated with seq
   */
  def put(writeHandle: RandomAccessFile, seq: Long, offset: Long) {
    val byteBuf = ByteBuffer.allocate(24)
    byteBuf.putLong(8, seq).putLong(16, offset)

    val chksum = checksummer.checksum(byteBuf.array)
    byteBuf.putLong(0, chksum)

    writeHandle.write(byteBuf.array)
  }

  /**
   * Load all sequence -> offset mappings from the input file handle.
   *
   * Has the side effect of advancing the file pointer to the end of
   * the file.
   *
   * Not thread safe with respect to indexFileHandle
   *
   * @param indexFileHandle the file handle to read from
   *
   * @return the SortedMap[Long, Long] of sequence -> offset mappings
   */
  // XXX: this may better serve as part of PersistedSeqIndex, or something
  //  else, as it is slightly higher level and can be composed of file ops
  //  public api, but that's for later
  def loadIndex(indexFileHandle: RandomAccessFile): JTreeMap[Long, Long] = {
    val byteBuf = ByteBuffer.allocate(bufferSize)

    readIndex(indexFileHandle, byteBuf)
  }

  /**
   * Read an entry off of a handle, with the side effect of advancing
   * the handle.
   *
   * It is the caller's duty to ensure that the handle is properly located,
   * aligned, and that data is available.
   *
   * Not thread safe with respect to indexFileHandle
   *
   * @indexFileHandle RandomAccessFile for the index file, it's offset
   *                    will be advanced 24 bytes (entry length)
   */
  def readEntry(indexFileHandle: RandomAccessFile): (Long, Long) = {
    val byteBuf = ByteBuffer.allocate(24)
    indexFileHandle.read(byteBuf.array)
    readEntry(byteBuf)
  }

  @tailrec
  private def readIndex(indexFileHandle: RandomAccessFile,
                        byteBuf: ByteBuffer,
                        soFar: JTreeMap[Long, Long] = new JTreeMap[Long, Long]): JTreeMap[Long, Long] = {
    val bytesRead = indexFileHandle.read(byteBuf.array)
    if (bytesRead > 0) {
      val chunk = decodeChunk(byteBuf, bytesRead)
      chunk.foreach((seqOff) => soFar.put(seqOff._1, seqOff._2))

      if (bytesRead == byteBuf.limit) {
        readIndex(indexFileHandle, byteBuf, soFar)
      } else {
        soFar
      }
    } else {
      soFar
    }
  }

  private def decodeChunk(byteBuf: ByteBuffer, chunkSize: Int): List[(Long, Long)] = {
    var chunk = List[(Long, Long)]()
    byteBuf.position(0)

    val entryBuf = ByteBuffer.allocate(24)

    while (byteBuf.position != chunkSize) {
      byteBuf.get(entryBuf.array)
      val (seq, offset) = readEntry(entryBuf)
      chunk ::= (seq, offset)
    }

    chunk
  }

  // reads an entry (destructively) from entryBuf, at entryBuf's current posisition,
  //  advancing the position past the entry
  private def readEntry(entryBuf: ByteBuffer): (Long, Long) = {
    val chksum = entryBuf.getLong(0)
    entryBuf.putLong(0, 0L)
    if (chksum != checksummer.checksum(entryBuf.array)) {
      throw new IllegalStateException("Sequence cache corrupted")
    }

    val seq = entryBuf.getLong(8)
    val offset = entryBuf.getLong(16)
    (seq, offset)
  }
}
