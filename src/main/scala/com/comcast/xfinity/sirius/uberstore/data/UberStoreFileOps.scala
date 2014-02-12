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

/**
 * Trait providing low level File operations for an UberStore
 */
trait UberStoreFileOps {

  /**
   * Write body into writeHandle using standard entry encoding
   * procedures.
   *
   * Has the side effect of advancing writeHandle to the end
   * of the written data
   *
   * @param writeHandle the file to write the body into
   * @param body the body of what we want stored
   *
   * @return offset this object was stored at
   */
  def put(writeHandle: RandomAccessFile, body: Array[Byte]): Long

  /**
   * Read the next entry out of the file, from the current offset
   *
   * Has the side effect of advancing readHandle to the end of
   * the written data
   *
   * @param readHandle the RandomAccessFile to read from, at the
   *          current offset
   *
   * @return Some(bytes) or None if EOF encountered
   */
  def readNext(readHandle: RandomAccessFile): Option[Array[Byte]]
}
