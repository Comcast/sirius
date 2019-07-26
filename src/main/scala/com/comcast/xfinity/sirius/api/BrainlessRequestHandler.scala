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

/**
  * Special instance of [[RequestHandler]] that indicates to Sirius to not bootstrap the in-memory brain
  */
case object BrainlessRequestHandler extends RequestHandler {
  /**
    * Handle a GET request
    *
    * @param key String identifying the search query
    * @return a SiriusResult wrapping the result of the query
    */
  override def handleGet(key: String): SiriusResult = SiriusResult.none()

  /**
    * Handle a PUT request
    *
    * @param key  unique identifier for the item to which the
    *             operation is being applied
    * @param body data passed in along with this request used
    *             for modifying the state at key
    * @return a SiriusResult wrapping the result of the operation.
    *         This should almost always be SiriusResult.none().
    *         In the future the API may be modified to return void.
    */
  override def handlePut(key: String, body: Array[Byte]): SiriusResult = SiriusResult.none()

  /**
    * Handle a DELETE request
    *
    * @param key unique identifier for the item to which the
    *            operation is being applied
    * @return a SiriusResult wrapping the result of the operation.
    *         This should almost always be SiriusResult.none().
    *         In the future the API may be modified to return void.
    */
  override def handleDelete(key: String): SiriusResult = SiriusResult.none()
}
