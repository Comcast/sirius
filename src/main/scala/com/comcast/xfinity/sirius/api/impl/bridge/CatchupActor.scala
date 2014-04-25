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
package com.comcast.xfinity.sirius.api.impl.bridge

import akka.actor.{ReceiveTimeout, Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.{NewLogSubrange => LogSubrange, GetLogSubrange}
import scala.concurrent.duration.FiniteDuration

case object CatchupRequestFailed
case class CatchupRequestSucceeded(logSubrange: LogSubrange)

object CatchupActor {
  /**
   * Build a Props object for a new CatchupActor.
   *
   * @param startSeq First sequence number to request
   * @param window number of events to request
   * @param timeout duration of timeout
   * @param source target ActorRef for request
   * @return
   */
  def props(startSeq: Long, window: Int, timeout: FiniteDuration, source: ActorRef): Props = {
    Props(classOf[CatchupActor], source, startSeq, startSeq + window, timeout)
  }
}

/**
 * Actor that handles a single catch-up request.
 */
private[bridge] class CatchupActor(source: ActorRef, nextSeq: Long, endSeq: Long, timeout: FiniteDuration) extends Actor {

  context.setReceiveTimeout(timeout)
  source ! GetLogSubrange(nextSeq, endSeq)

  def receive = {
    case logSubrange: LogSubrange =>
      context.parent ! CatchupRequestSucceeded(logSubrange)
      context.stop(self)

    case ReceiveTimeout =>
      context.parent ! CatchupRequestFailed
      context.stop(self)
  }
}
