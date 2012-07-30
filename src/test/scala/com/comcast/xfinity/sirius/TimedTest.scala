package com.comcast.xfinity.sirius

import annotation.tailrec

/**
 * This trait contains conveniences to facilitate timing related tests
 */
trait TimedTest {

  /**
   * Wait for condition specified by pred, a call by name argument, to become true, or return
   * false if it does not do so in time
   *
   * @param pred the predicate to await becoming true
   * @param timeout how long to wait in milliseconds for the condition to become true
   * @param waitBetween how long to wait in between checks
   */
  @tailrec
  final def waitForTrue(pred: => Boolean, timeout: Long,  waitBetween: Long): Boolean = {
    if (timeout < 0){
      false
    } else if (pred){
      true
    } else {
      Thread.sleep(waitBetween)
      waitForTrue(pred, timeout - waitBetween, waitBetween)
    }
  }

}