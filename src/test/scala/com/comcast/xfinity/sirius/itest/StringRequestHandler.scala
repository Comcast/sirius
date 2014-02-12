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

package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.api.RequestHandler
import collection.mutable.HashMap
import com.comcast.xfinity.sirius.api.SiriusResult

class StringRequestHandler extends RequestHandler {

  var cmdsHandledCnt = 0;
  val map: HashMap[String, Array[Byte]] = new HashMap[String, Array[Byte]]()

  /**
   * Handle a GET request
   */
  def handleGet(key: String): SiriusResult = {
    cmdsHandledCnt += 1;
    map.get(key) match {
      case Some(v) => SiriusResult.some(v)
      case None => SiriusResult.none()
    }
  }


  /**
   * Handle a PUT request
   */
  def handlePut(key: String, body: Array[Byte]): SiriusResult = {
    cmdsHandledCnt += 1;
    map.put(key, body) match {
      case Some(v) => SiriusResult.some(v)
      case None => SiriusResult.none()
    }
  }

  /**
   * Handle a DELETE request
   */
  def handleDelete(key: String): SiriusResult = {
    cmdsHandledCnt += 1;
    map.remove(key) match {
      case Some(v) => SiriusResult.some(v)
      case None => SiriusResult.none()
    }
  }
}
