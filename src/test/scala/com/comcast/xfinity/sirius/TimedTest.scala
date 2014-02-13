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
