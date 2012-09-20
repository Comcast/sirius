package com.comcast.xfinity.sirius.uberstore.seqindex

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.{TreeMap => JTreeMap}
import com.comcast.xfinity.sirius.uberstore.common.Checksummer



/**
 * Class providing low level file operations for a binary
 * based sequence index with checksum based data protection.
 *
 * @param checksummer Checksummer used to calculate entry checksums
 * @param bufferSize size of the buffer to be used for reading from passed
 *          in handles.  Each read operation will have its own buffer
 */
class SeqIndexBinaryFileOps(checksummer: Checksummer,
                            bufferSize: Int = 24 * 1024) extends SeqIndexFileOps {

  /**
   * @inheritdoc
   */
  def put(writeHandle: RandomAccessFile, seq: Long, offset: Long) {
    val byteBuf = ByteBuffer.allocate(24)
    byteBuf.putLong(8, seq).putLong(16, offset)

    val chksum = checksummer.checksum(byteBuf.array)
    byteBuf.putLong(0, chksum)

    writeHandle.write(byteBuf.array)
  }

  /**
   * @inheritdoc
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