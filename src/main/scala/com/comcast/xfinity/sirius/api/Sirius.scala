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
package com.comcast.xfinity.sirius.api

import java.util.concurrent.Future

trait Sirius {
  
  /**
   * Enqueue a GET for processing
   */
  def enqueueGet(key: String): Future[SiriusResult]
  
  /**
   * Enqueue a PUT for processing
   */
  def enqueuePut(key: String, body: Array[Byte]): Future[SiriusResult]

  /**
   * Enqueue a DELETE for processing
   */
  def enqueueDelete(key: String): Future[SiriusResult]

  /**
   * Is the system ready to handle requests?
   *
   * @return true if so, false if not
   */
  def isOnline: Boolean
}
