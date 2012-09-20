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
 */
class SeqIndexBinaryFileOps(checksummer: Checksummer) extends SeqIndexFileOps {

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
    val result = new JTreeMap[Long, Long]()
    val byteBuf = ByteBuffer.allocate(24)

    while (indexFileHandle.getFilePointer != indexFileHandle.length) {
      indexFileHandle.readFully(byteBuf.array)

      val chksum = byteBuf.getLong(0)
      byteBuf.putLong(0, 0L)
      if (chksum != checksummer.checksum(byteBuf.array)) {
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