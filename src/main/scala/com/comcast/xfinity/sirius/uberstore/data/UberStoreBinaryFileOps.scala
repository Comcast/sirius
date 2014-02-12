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
package com.comcast.xfinity.sirius.uberstore.data

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import com.comcast.xfinity.sirius.uberstore.common.Checksummer

/**
 * Class providing UberStoreFileOps, storing entries in the following format:
 *
 *    [len: Int][chksum: Long][data: Array[Byte]]
 */
class UberStoreBinaryFileOps extends UberStoreFileOps {
    this: Checksummer =>

  final val HEADER_SIZE = 4 + 8 // int len + long checksum

  /**
   * @inheritdoc
   */
  def put(writeHandle: RandomAccessFile, body: Array[Byte]): Long = {
    val offset = writeHandle.getFilePointer

    val len: Int = body.length
    val chksum: Long = checksum(body)

    val byteBuf = ByteBuffer.allocate(HEADER_SIZE + len)

    byteBuf.putInt(len).putLong(chksum).put(body)

    writeHandle.write(byteBuf.array)

    offset
  }

  /**
   * @inheritdoc
   */
  def readNext(readHandle: RandomAccessFile): Option[Array[Byte]] = {
    val offset = readHandle.getFilePointer

    if (offset == readHandle.length) { // EOF
      None
    } else {
      val (entryLen, chksum) = readHeader(readHandle)

      val body = readBody(readHandle, entryLen)
      if (chksum == checksum(body)) {
        Some(body) // [that i used to know | to love]
      } else {
        throw new IllegalStateException("File corrupted at offset " + offset)
      }
    }
  }


  // Helper jawns
  private def readHeader(readHandle: RandomAccessFile): (Int,  Long) = {
    val entryHeaderBuf = ByteBuffer.allocate(HEADER_SIZE)
    readHandle.readFully(entryHeaderBuf.array)

    (entryHeaderBuf.getInt(), entryHeaderBuf.getLong())
  }

  private def readBody(readHandle: RandomAccessFile, bodyLen: Int): Array[Byte] = {
    val entryBuf = ByteBuffer.allocate(bodyLen)
    readHandle.readFully(entryBuf.array)

    entryBuf.array
  }
}
