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
package com.comcast.xfinity.sirius.uberstore.segmented

import java.io.File

object FlagFile {
  /**
   * Instantiate a persistent boolean flag at the given location.
   *
   * @param location String location of flag
   */
  def apply(location: String) = {
    new FlagFile(new File(location))
  }
}

/**
 * Persistent boolean flag at the given location.
 *
 * @param location location of flag
 */
private[segmented] class FlagFile(location: File) {
  /**
   * Get current value of flag.
   * @return
   */
  def value = location.exists

  /**
   * Set the value of the flag.
   * @param value new value
   */
  def set(value: Boolean) {
    value match {
      case true =>
        location.createNewFile()
      case false =>
        location.delete()
    }
  }
}
