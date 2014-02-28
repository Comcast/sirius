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
package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.api.impl.OrderedEvent

/**
 * Defines the Log Writer
 */
trait SiriusLog {

  /**
   * Write an entry to the log
   *
   * @param entry the OrderedEvent to write to the log
   */
  def writeEntry(entry: OrderedEvent)

  /**
   * Apply fun to each entry in the log in order
   *
   * @param fun function to apply
   */
  def foreach[T](fun: OrderedEvent => T): Unit = foldLeft(())((_, e) => fun(e))

  /**
   * Fold left across the log entries
   * @param acc0 initial accumulator value
   * @param foldFun function to apply to the log entry, the result being the new accumulator
   */
  def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T = foldLeftRange(0, Long.MaxValue)(acc0)(foldFun)

  /**
   * Fold left across a specified range of log entries
   * @param startSeq sequence number to start with, inclusive
   * @param endSeq sequence number to end with, inclusive
   * @param acc0 initial accumulator value
   * @param foldFun function to apply to the log entry, the result being the new accumulator
   */
  def foldLeftRange[T](startSeq: Long, endSeq: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T

  /**
   * retrieves the next sequence number to be written
   */
  def getNextSeq: Long

  def compact()

  /**
   * Calculates size of the SiriusLog.
   * @return a measure of size of the SiriusLog
   */
  def size: Long
}
