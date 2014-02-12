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
package com.comcast.xfinity.sirius.api.impl.paxos

object Ballot {
  val empty = Ballot(Int.MinValue, "")
}

case class Ballot(seq: Int, leaderId: String) extends Ordered[Ballot] {
  def compare(that: Ballot) = that match {
    case Ballot(thatSeq, _) if seq < thatSeq => -1
    case Ballot(thatSeq, _) if seq > thatSeq => 1
    case Ballot(_, thatLeaderId) if leaderId < thatLeaderId => -1
    case Ballot(_, thatLeaderId) if leaderId > thatLeaderId => 1
    case _ => 0
  }
}
