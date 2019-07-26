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

/**
  * An Uberstore data file handle factory that creates read file handles that use buffered read operations
  * @param readBufferSize the size of the buffer for read operations
  */
private [uberstore] class BufferedReadFileHandleFactory(readBufferSize: Int) extends UberDataFileHandleFactory {

  /** @inheritdoc */
  override def createWriteHandle(dataFileName: String) = RandomAccessFileWriteHandle(dataFileName)

  /**
    * Creates a file handle for reading from an Uberstore data file using buffered operations
    * @param dataFileName the absolute path to the Uberstore data file
    * @param baseOffset the offset within the Uberstore data file at which read operations will begin
    * @return the file handle
    */
  override def createReadHandle(dataFileName: String, baseOffset: Long) = BufferedFileReadHandle(dataFileName, baseOffset, readBufferSize)
}
