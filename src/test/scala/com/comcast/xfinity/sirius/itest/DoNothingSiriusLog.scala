/*
 *  Copyright 2012-2013 Comcast Cable Communications Management, LLC
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

import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import org.slf4j.LoggerFactory
import com.comcast.xfinity.sirius.api.impl.OrderedEvent

class DoNothingSiriusLog extends SiriusLog {
  private val logger = LoggerFactory.getLogger(classOf[DoNothingSiriusLog])

  override def writeEntry(entry: OrderedEvent) {
    logger.info("Writing entry for {}", entry)
  }

  override def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T = acc0

  override def foldLeftRange[T](startSeq: Long, endSeq: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T = {
    acc0
  }

  override def getNextSeq = 1L

  override def compact() {}
}
