package com.comcast.xfinity.sirius.uberstore.data

import java.io.RandomAccessFile
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.uberstore._
import annotation.tailrec

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
    val fileOps = new UberStoreBinaryFileOps with Fnv1aChecksummer
    val codec = new BinaryEventCodec
    new UberDataFile(dataFileName, fileOps, codec) with HandleProvider {
      def createWriteHandle(fname: String) = new RandomAccessFile(fname, "rw")
      def createReadHandle(fname: String) = new RandomAccessFile(fname, "r")
    }
  }

  /**
   * Private trait so that we can abstract out creating RandomAcesssFiles
   * for testing.
   */
  private[data] trait HandleProvider {
    def createWriteHandle(fname: String): RandomAccessFile
    def createReadHandle(fname: String): RandomAccessFile
  }
}

/**
 * Lower level file access for UberStore data files.
 *
 * @param dataFileName the file for this object to encapsulate
 * @param fileOps service class providing low level file operations
 * @param codec OrderedEventCodec for transforming OrderedEvents
 */
// TODO: use trait to hide this constructor but keep type visible?
private[uberstore] class UberDataFile(dataFileName: String,
                                      fileOps: UberStoreFileOps,
                                      codec: OrderedEventCodec) {
    this: UberDataFile.HandleProvider =>

  val writeHandle = createWriteHandle(dataFileName)
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
    val readHandle = createReadHandle(dataFileName)
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