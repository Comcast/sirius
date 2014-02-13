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

package com.comcast.xfinity.sirius

import api.{SiriusResult, RequestHandler}
import java.util.concurrent.{TimeUnit, CountDownLatch}

/**
 * Request handler for testing purposes that includes a countdown
 * latch so that we can monitor it for completion of events
 */
class LatchedRequestHandler(expectedTicks: Int) extends RequestHandler {
  var latch = new CountDownLatch(expectedTicks)

  def handleGet(key: String): SiriusResult = SiriusResult.none()

  def handlePut(key: String, body: Array[Byte]): SiriusResult = {
    latch.countDown()
    SiriusResult.none()
  }

  def handleDelete(key: String): SiriusResult = {
    latch.countDown()
    SiriusResult.none()
  }

  def await(timeout: Long, timeUnit: TimeUnit) {
    latch.await(timeout, timeUnit)
  }

  def resetLatch(newExpectedTicks: Int) {
    latch = new CountDownLatch(newExpectedTicks)
  }
}

