package com.comcast.xfinity.sirius

import annotation.tailrec

/**
 * This trait contains conveniences to facilitate timing related tests
 */
trait TimedTest {

  @tailrec
  val waitForTrue: ((Any=>Boolean),Long, Long)=>Boolean = (test: (Any => Boolean), timeout: Long, waitBetween: Long) => {
    if (timeout < 0) {
      false

    } else if (test()) {
      true
    } else {
      Thread.sleep(waitBetween)
      waitForTrue(test, timeout - waitBetween, waitBetween)
    }

  }

}