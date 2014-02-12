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
package com.comcast.xfinity.sirius.uberstore.common

/**
 * Trait supplying checksumming capabilities
 */
trait Checksummer {

  /**
   * Given an array of bytes will calculate a Long checksum
   * value for them
   *
   * @param bytes Array[Byte] to checksum
   *
   * @return the checksum as a Long
   */
  def checksum(bytes: Array[Byte]): Long
}
