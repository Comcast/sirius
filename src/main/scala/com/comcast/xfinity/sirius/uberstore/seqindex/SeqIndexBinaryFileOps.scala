package com.comcast.xfinity.sirius.uberstore.seqindex

import java.io.RandomAccessFile
import com.comcast.xfinity.sirius.uberstore.Checksummer
import java.nio.ByteBuffer
import java.util.{TreeMap => JTreeMap}

/**
 * Class providing low level file operations for a binary
 * based sequence index with checksum based data protection.
 */
class SeqIndexBinaryFileOps extends SeqIndexFileOps {
    this: Checksummer =>

  /**
   * @inheritdoc
   */
  def put(writeHandle: RandomAccessFile, seq: Long, offset: Long) {
    val byteBuf = ByteBuffer.allocate(24)
    byteBuf.putLong(8, seq).putLong(16, offset)

    val chksum = checksum(byteBuf.array)
    byteBuf.putLong(0, chksum)

    writeHandle.write(byteBuf.array)
  }

  /**
   * @inheritdoc
   */
  def loadIndex(indexFileHandle: RandomAccessFile): JTreeMap[Long, Long] = {
    val result = new JTreeMap[Long, Long]()
    val byteBuf = ByteBuffer.allocate(24)

    while (indexFileHandle.getFilePointer != indexFileHandle.length) {
      indexFileHandle.readFully(byteBuf.array)

      val chksum = byteBuf.getLong(0)
      byteBuf.putLong(0, 0L)
      if (chksum != checksum(byteBuf.array)) {
        throw new IllegalStateException("Sequence cache corrupted at " +
          (indexFileHandle.getFilePointer - 24))
      }

      val seq = byteBuf.getLong(8)
      val offset = byteBuf.getLong(16)
      result.put(seq, offset)
    }

    result
  }

}