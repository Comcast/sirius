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

import java.lang.Object

object Fnv1aChecksummer {

  /**
   * Get an object with a Fnv1aChecksummer
   *
   * For the times that you don't want to do a mixin
   */
  def apply() = new Object with Fnv1aChecksummer
}

/**
 * Trait supplying the FNV-1a checksum algorithm, as defined by
 *
 * http://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function#FNV-1a_hash
 */
trait Fnv1aChecksummer extends Checksummer {
  final val fnvOffsetBasis = -3750763034362895579L
  final val fnvPrime = 1099511628211L

  /**
   * @inheritdoc
   */
  def checksum(bytes: Array[Byte]): Long = {
    var hash = fnvOffsetBasis
    var i = 0

    while (i < bytes.length) {
      hash = fnvPrime * (hash ^ (bytes(i) & 0x0ff))
      i += 1
    }

    hash
  }
}
