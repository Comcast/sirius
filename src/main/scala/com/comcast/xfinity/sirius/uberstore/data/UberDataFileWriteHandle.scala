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
  * Trait providing abstraction of low level write operations to an Uberstore
  */
trait UberDataFileWriteHandle extends AutoCloseable {
  /**
    * Returns the current offset within the file
    * @return the offset
    */
  def offset(): Long

  /**
    * Writes the array of bytes to the file
    * @param bytes to write
    * @return the offset within the file at which the bytes were written
    */
  def write(bytes: Array[Byte]): Long
}
