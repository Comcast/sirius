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
package com.comcast.xfinity.sirius.tool.format

import com.comcast.xfinity.sirius.api.impl.OrderedEvent

object OrderedEventFormatter {

  private val defaultFormatter = "com.comcast.xfinity.sirius.tool.format.DefaultFormatter"

  val formatter: OrderedEventFormatter = {
    val formatterClass = System.getProperty("eventFormatter", defaultFormatter)
    val clazz = Class.forName(formatterClass)
    clazz.newInstance().asInstanceOf[OrderedEventFormatter]
  }

  def printEvent(evt: OrderedEvent) {
    println(formatter.format(evt))
  }
}

/**
 * Trait/interface for pretty formatting ordered events
 */
trait OrderedEventFormatter {
  def format(evt: OrderedEvent): String
}
