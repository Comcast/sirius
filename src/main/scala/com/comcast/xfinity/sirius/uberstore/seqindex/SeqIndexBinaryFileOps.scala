package com.comcast.xfinity.sirius.uberstore.seqindex

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.{TreeMap => JTreeMap}
import com.comcast.xfinity.sirius.uberstore.common.Checksummer
import com.comcast.xfinity.sirius.uberstore.common.Fnv1aChecksummer

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
  def loadIndex(indexFileHandle: RandomAccessFile): JTreeMap[Long, Long] = {
    val byteBuf = ByteBuffer.allocate(bufferSize)

    readIndex(indexFileHandle, byteBuf)
  }

  private def readIndex(indexFileHandle: RandomAccessFile,
                        byteBuf: ByteBuffer,
                        soFar: JTreeMap[Long, Long] = new JTreeMap[Long, Long]): JTreeMap[Long, Long] = {
    val bytesRead = indexFileHandle.read(byteBuf.array)
    if (bytesRead > 0) {
      soFar.putAll(decodeChunk(byteBuf, bytesRead))
      if (bytesRead == byteBuf.limit) {
        readIndex(indexFileHandle, byteBuf, soFar)
      } else {
        soFar
      }
    } else {
      soFar
    }
  }

  private def decodeChunk(byteBuf: ByteBuffer, chunkSize: Int): JTreeMap[Long, Long] = {
    val chunk = new JTreeMap[Long, Long]
    byteBuf.position(0)

    val entryBuf = ByteBuffer.allocate(24)

    while (byteBuf.position != chunkSize) {
      byteBuf.get(entryBuf.array)
      updateWithEntry(chunk, entryBuf)
    }

    chunk
  }

  private def updateWithEntry(toUpdate: JTreeMap[Long, Long], entryBuf: ByteBuffer) {
    val chksum = entryBuf.getLong(0)
    entryBuf.putLong(0, 0L)
    if (chksum != checksummer.checksum(entryBuf.array)) {
      throw new IllegalStateException("Sequence cache corrupted")
    }

    val seq = entryBuf.getLong(8)
    val offset = entryBuf.getLong(16)
    toUpdate.put(seq, offset)
  }
}