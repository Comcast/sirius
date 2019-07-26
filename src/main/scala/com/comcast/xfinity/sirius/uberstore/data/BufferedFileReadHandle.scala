/*
 *  Copyright 2012-2019 Comcast Cable Communications Management, LLC
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

import java.io.{BufferedInputStream, DataInputStream, InputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import scala.annotation.tailrec

object BufferedFileReadHandle {

  /**
    * Creates a new buffered sequential access [[UberDataFileReadHandle]] for the file
    *
    * @param dataFileName the name of the file
    * @param baseOffset the initial offset
    * @param readBufferSize the buffer size
    */
  def apply(dataFileName: String, baseOffset:Long, readBufferSize: Int): UberDataFileReadHandle = {
    val randomAccessFile = new RandomAccessFile(dataFileName, "r")
    randomAccessFile.seek(baseOffset)
    val fileChannel = randomAccessFile.getChannel
    val fileChannelInputStream = new FileChannelInputStream(fileChannel)
    val inputStream = new BufferedInputStream(fileChannelInputStream, readBufferSize)
    new BufferedFileReadHandle(inputStream, baseOffset)
  }
}

/**
  * Implementation of an Uberstore read file handle that uses buffered read operations
  * @param inputStream the input stream containing the contents of the file
  * @param baseOffset the offset from which read operations will start
  */
private[uberstore] class BufferedFileReadHandle(val inputStream: InputStream, val baseOffset: Long) extends UberDataFileReadHandle {
  private val dataInputStream: DataInputStream = new DataInputStream(inputStream)
  private var currentOffset: Long = baseOffset

  /** @inheritdoc */
  def offset(): Long = currentOffset

  /** @inheritdoc */
  override def eof(): Boolean = {
    inputStream.mark(1)
    try {
      inputStream.read() == -1
    } finally {
      inputStream.reset()
    }
  }

  /** @inheritdoc */
  def readInt(): Int = {
    val result = dataInputStream.readInt()
    currentOffset += 4
    result
  }

  /** @inheritdoc */
  def readLong(): Long = {
    val result = dataInputStream.readLong()
    currentOffset += 8
    result
  }

  /** @inheritdoc */
  def readFully(array: Array[Byte]): Unit = {
    dataInputStream.readFully(array)
    currentOffset += array.length
  }

  /** @inheritdoc */
  def close(): Unit = dataInputStream.close()
}

/**
  * Helper class that wraps a [[java.nio.channels.FileChannel]] as an [[java.io.InputStream]] so that it can be
  * used with [[java.io.BufferedInputStream]]
  */
private [uberstore] class FileChannelInputStream(private val channel: FileChannel) extends InputStream {
  private val singleByteBuffer = ByteBuffer.allocate(1)

  /**
    * Reads the next byte from the channel if it is available, or returns -1.
    * This method is required and here for completeness, but all calls from [[BufferedInputStream]]
    * use [[java.io.InputStream#read(Array[Byte],Int,Int):Unit*]] so this method should never get called.
    * @return the Int value of the Byte in the range of 0-255, or -1 if EOF
    */
  @tailrec
  override final def read(): Int = {
    singleByteBuffer.reset()
    channel.read(singleByteBuffer) match {
      case -1 => -1
      case 0 => read()
      case _ =>
        singleByteBuffer.flip()
        singleByteBuffer.get()
    }
  }

  override def read(b: Array[Byte]): Int = channel.read(ByteBuffer.wrap(b))

  override def read(b: Array[Byte], off: Int, len: Int): Int = channel.read(ByteBuffer.wrap(b, off, len))

  override def skip(n: Long): Long = {
    val newPosition = channel.position() + n
    channel.position(newPosition)
    newPosition
  }

  override def close(): Unit = channel.close()
}
