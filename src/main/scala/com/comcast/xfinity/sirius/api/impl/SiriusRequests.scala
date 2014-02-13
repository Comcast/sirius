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
package com.comcast.xfinity.sirius.api.impl

import java.util.Arrays

sealed trait SiriusRequest {
  def key: String
}

case class Get(key: String) extends SiriusRequest

sealed trait NonCommutativeSiriusRequest extends SiriusRequest

// XXX: hashCode may not be reliable due to Array[Byte] from Java sucking
case class Put(key: String, body: Array[Byte]) extends NonCommutativeSiriusRequest {
  override def equals(that: Any) = that match {
    case Put(`key`, thatBody) if Arrays.equals(body, thatBody) => true
    case _ => false
  }
}

case class Delete(key: String) extends NonCommutativeSiriusRequest
