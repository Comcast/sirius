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

import com.comcast.xfinity.sirius.api.SiriusConfiguration

object UberDataFileHandleFactory {

  /**
    * Creates a [[UberDataFileHandleFactory]] that can create Uberstore data file handles based on the configuration
    * @param siriusConfig the Sirius configuration
    * @return the factory for creating file handles
    */
  def apply(siriusConfig: SiriusConfiguration): UberDataFileHandleFactory = {
    val LOG_USE_READ_BUFFER = siriusConfig.getProp(SiriusConfiguration.LOG_USE_READ_BUFFER, false)
    val LOG_READ_BUFFER_SIZE = siriusConfig.getProp(SiriusConfiguration.LOG_READ_BUFFER_SIZE_BYTES, 8 * 1024)

    apply(LOG_USE_READ_BUFFER, LOG_READ_BUFFER_SIZE)
  }

  /**
    * Creates an [[UberDataFileHandleFactory]] that can create Uberstore data file handles based on specific configuration values
    * @param useReadBuffer `true` to use buffered read operations
    * @param readBufferSize the size of the read buffer when using buffered read operations
    * @return the factory for creating file handles
    */
  def apply(useReadBuffer: Boolean, readBufferSize: Int): UberDataFileHandleFactory = {
    if (useReadBuffer && readBufferSize > 0) {
      new BufferedReadFileHandleFactory(readBufferSize)
    } else {
      RandomAccessFileHandleFactory
    }
  }
}

/**
  * Trait providing abstraction to obtaining file handles for read and write operations to an Uberstore data file
  */
trait UberDataFileHandleFactory {

  /**
    * Creates a file handle for writing to an Uberstore data file
    * @param dataFileName the absolute path to the Uberstore data file
    * @return the file handle
    */
  def createWriteHandle(dataFileName: String): UberDataFileWriteHandle

  /**
    * Creates a file handle for reading from an Uberstore data file
    * @param dataFileName the absolute path to the Uberstore data file
    * @param baseOffset the offset within the Uberstore data file at which read operations will begin
    * @return the file handle
    */
  def createReadHandle(dataFileName: String, baseOffset: Long): UberDataFileReadHandle
}
