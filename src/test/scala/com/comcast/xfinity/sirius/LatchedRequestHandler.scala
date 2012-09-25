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

