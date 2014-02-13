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
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import annotation.tailrec
import com.comcast.xfinity.sirius.uberstore.common.Fnv1aChecksummer

object UberDataFile {

  /**
   * Create a fully wired UberDataFile
   *
   * Uses:
   *  - UberStoreBinaryFileOps for file operations
   *  - Fnv1aChecksummer for checksums
   *  - BinaryEventCodec for encoding events
   *
   * @param dataFileName the data file name, this will
   *          be created if it does not exist
   *
   * @return fully constructed UberDataFile
   */
  def apply(dataFileName: String) = {
    val uberFileDesc = new UberFileDesc(dataFileName)
    val fileOps = new UberStoreBinaryFileOps with Fnv1aChecksummer
    val codec = new BinaryEventCodec
    new UberDataFile(uberFileDesc, fileOps, codec)
  }

  /**
   * Private class for delivering file handles for an UberDataFile,
   * need this because write handles are created on demand.
   *
   * THIS IS PRIVATE TO UBERDATAFILE, DON'T MESS WITH IT
   *
   * @param dataFileName the name of the file for this descriptor to wrap
   */
  private[data] class UberFileDesc(dataFileName: String) {
    /**
     * Construct and return a writable RandomAccessFile
     *
     * Has the side effect of opening a file descriptor, this must be
     * closed by the caller.
     *
     * @return read/write RandomAccessFile (note write only is not allowed)
     */
    def createWriteHandle() = new RandomAccessFile(dataFileName, "rw")

    /**
     * Construct a read only RandomAccessFile
     *
     * Has the side effect of opening a file descriptor, this must be
     * closed by the caller.
     *
     * @return read only RandomAccessFile
     */
    def createReadHandle() = new RandomAccessFile(dataFileName, "r")
  }
}

/**
 * Lower level file access for UberStore data files.
 *
 * @param uberFileDesc the UberDataFile.UberFileDesc to provide handles
 *          to the underlying file. Extracted out for testing.
 * @param fileOps service class providing low level file operations
 * @param codec OrderedEventCodec for transforming OrderedEvents
 */
// TODO: use trait to hide this constructor but keep type visible?
private[uberstore] class UberDataFile(uberFileDesc: UberDataFile.UberFileDesc,
                                      fileOps: UberStoreFileOps,
                                      codec: OrderedEventCodec) {

  val writeHandle = uberFileDesc.createWriteHandle()
  writeHandle.seek(writeHandle.length)

  var isClosed = false

  /**
   * Write an event to this file
   *
   * @param event the OrderedEvent to persist
   */
  def writeEvent(event: OrderedEvent): Long = {
    if (isClosed) {
      throw new IllegalStateException("Attempting to write to closed UberDataFile")
    }

    fileOps.put(writeHandle, codec.serialize(event))
  }

  /**
   * Fold left over this entire file
   *
   * @param acc0 initial accumulator value
   * @param foldFun fold function
   */
  def foldLeft[T](acc0: T)(foldFun: (T, Long, OrderedEvent) => T): T = {
    foldLeftRange(0, Long.MaxValue)(acc0)(foldFun)
  }

  /**
   * Fold left starting at baseOffset, until the file pointer is at or beyond endOff.
   *
   * The caller is expected to put in a sane baseOff which corresponds with the start
   * of an actual event.
   *
   * This is a low low level API function that should not be taken lightly
   *
   * @param baseOff starting offset in the file to start at
   * @param endOff offset at or after which the operation should conclude, inclusive
   * @param acc0 initial accumulator value
   * @param foldFun the fold function
   *
   * @return T the final accumulator value
   */
  def foldLeftRange[T](baseOff: Long, endOff: Long)(acc0: T)(foldFun: (T, Long, OrderedEvent) => T): T = {
    val readHandle = uberFileDesc.createReadHandle()
    try {
      readHandle.seek(baseOff)
      foldLeftUntil(readHandle, endOff, acc0, foldFun)
    } finally {
      readHandle.close()
    }
  }

  // private low low low level fold left
  @tailrec
  private def foldLeftUntil[T](readHandle: RandomAccessFile, maxOffset: Long, acc: T, foldFun: (T, Long, OrderedEvent) => T): T = {
    val offset = readHandle.getFilePointer
    if (offset > maxOffset) {
      acc
    } else {
      fileOps.readNext(readHandle) match {
        case None => acc
        case Some(bytes) =>
          val accNew = foldFun(acc, offset, codec.deserialize(bytes))
          foldLeftUntil(readHandle, maxOffset, accNew, foldFun)
      }
    }
  }

  /**
   * Close open file handles.  Only touching writeHandle here, since readHandles are opened and then
   * closed in a finally of the same block.  This UberDataFile should not be used after close is called.
   */
  def close() {
    if (!isClosed) {
      writeHandle.close()
      isClosed = true
    }
  }

  override def finalize() {
    close()
  }
}
