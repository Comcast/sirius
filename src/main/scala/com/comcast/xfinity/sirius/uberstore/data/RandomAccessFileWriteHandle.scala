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

object RandomAccessFileWriteHandle {

  /**
    * Creates an Uberstore data file write handle that uses random access write operations
    * @param dataFileName the absolute path to the Uberstore data file
    * @return the file handle
    */
  def apply(dataFileName: String): UberDataFileWriteHandle = {
    val randomAccessFile = new RandomAccessFile(dataFileName, "rw")
    randomAccessFile.seek(randomAccessFile.length())
    new RandomAccessFileWriteHandle(randomAccessFile)
  }
}

/**
  * Implementation of an Uberstore write file handle that uses random access write operations
  */
private [data] class RandomAccessFileWriteHandle(randomAccessFile: RandomAccessFile) extends UberDataFileWriteHandle {

  /** @inheritdoc */
  override def offset(): Long = randomAccessFile.getFilePointer

  /** @inheritdoc */
  override def write(bytes: Array[Byte]): Long = {
    val offset = randomAccessFile.getFilePointer
    randomAccessFile.write(bytes)
    offset
  }

  /** @inheritdoc */
  override def close(): Unit = randomAccessFile.close()
}
