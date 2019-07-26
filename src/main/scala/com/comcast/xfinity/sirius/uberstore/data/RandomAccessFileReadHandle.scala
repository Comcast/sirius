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

import java.io.RandomAccessFile

object RandomAccessFileReadHandle {

  /**
    * Creates a new random access [[UberDataFileReadHandle]] for the file
    *
    * @param dataFileName the name of the file
    * @param baseOffset the initial offset
    */
  def apply(dataFileName: String, baseOffset: Long): UberDataFileReadHandle = {
    val randomAccessFile = new RandomAccessFile(dataFileName, "r")
    randomAccessFile.seek(baseOffset)
    new RandomAccessFileReadHandle(randomAccessFile)
  }
}

/**
  * Implementation of an Uberstore read file handle that uses random access file operations
  */
private[uberstore] class RandomAccessFileReadHandle(private val randomAccessFile: RandomAccessFile) extends UberDataFileReadHandle {
  /** @inheritdoc */
  override def offset(): Long = randomAccessFile.getFilePointer
  /** @inheritdoc */
  override def eof(): Boolean = randomAccessFile.getFilePointer >= randomAccessFile.length()
  /** @inheritdoc */
  override def readInt(): Int = randomAccessFile.readInt()
  /** @inheritdoc */
  override def readLong(): Long = randomAccessFile.readLong()
  /** @inheritdoc */
  override def readFully(array: Array[Byte]): Unit = randomAccessFile.readFully(array)
  /** @inheritdoc */
  override def close(): Unit = randomAccessFile.close()
}
