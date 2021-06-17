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
  * Trait providing abstraction of low level read operations to an Uberstore
  */
trait UberDataFileReadHandle extends AutoCloseable {
  /**
    * Returns the current offset within the file
    * @return the offset
    */
  def offset(): Long

  /**
    * Returns whether the reader is at the end of the file
    * @return `true` if at the end of the file; otherwise, `false`
    */
  def eof(): Boolean

  /**
    * Reads the next 32-bit Integer from the file
    * @return the 32-bit Integer
    * @throws java.io.EOFException if EOF
    */
  def readInt(): Int

  /**
    * Reads the next 64-bit Long from the file
    * @return the 64-bit Long
    * @throws java.io.EOFException if EOF
    */
  def readLong(): Long

  /**
    * Populates the entire array with the next bytes from the file
    * @param array to populate
    * @throws java.io.EOFException if EOF
    */
  def readFully(array: Array[Byte]): Unit
}
