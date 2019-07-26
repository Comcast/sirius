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
  def put(writeHandle: UberDataFileWriteHandle, body: Array[Byte]): Long = {

    val len: Int = body.length
    val chksum: Long = checksum(body)

    val byteBuf = ByteBuffer.allocate(HEADER_SIZE + len)

    byteBuf.putInt(len).putLong(chksum).put(body)

    writeHandle.write(byteBuf.array)
  }

  /**
   * @inheritdoc
   */
  def readNext(readHandle: UberDataFileReadHandle): Option[Array[Byte]] =
    if (readHandle.eof()) {
      None
    } else {
      val (bodyLen, chksum) = readHeader(readHandle)
      val body = readBody(readHandle, bodyLen)
      if (chksum == checksum(body)) {
        Some(body) // [that i used to know | to love]
      } else {
        throw new IllegalStateException("File corrupted at offset " + readHandle.offset())
      }
    }


  // Helper jawns
  private def readHeader(readHandle: UberDataFileReadHandle): (Int, Long) =
    (readHandle.readInt(), readHandle.readLong())

  private def readBody(readHandle: UberDataFileReadHandle, bodyLen: Int): Array[Byte] = {
    val entry = new Array[Byte](bodyLen)
    readHandle.readFully(entry)
    entry
  }
}
