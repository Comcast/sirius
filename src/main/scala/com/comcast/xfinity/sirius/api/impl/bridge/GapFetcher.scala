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

import akka.actor.{Props, ReceiveTimeout, ActorRef, Actor}
import scala.concurrent.duration._
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.{GetLogSubrange, LogSubrange}
import com.comcast.xfinity.sirius.api.impl.bridge.PaxosStateBridge.RequestFromSeq
import scala.language.postfixOps

object GapFetcher {

  /**
   * Create Props for GapFetcher actor.
   *
   * @param firstGapSeq first sequence number to request; lower bound
   * @param target actor from whom we should ask for chunks
   * @param replyTo actor to send OrderedEvents to
   * @param config SiriusConfiguration for this node
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(firstGapSeq: Long, target: ActorRef, replyTo: ActorRef, config: SiriusConfiguration): Props = {
    val chunkSize = config.getProp(SiriusConfiguration.LOG_REQUEST_CHUNK_SIZE, 1000)
    val chunkReceiveTimeout = config.getProp(SiriusConfiguration.LOG_REQUEST_RECEIVE_TIMEOUT_SECS, 5)

    Props(classOf[GapFetcher], firstGapSeq, target, replyTo, chunkSize, chunkReceiveTimeout)
  }
}

/**
 * Actor responsible for requesting gaps from a specified target, with no upper bound.  As long
 * as the target can fulfill the request, the actor will continue requesting.
 *
 * @param firstGapSeq first sequence number to request; lower bound
 * @param target actor from whom we should ask for chunks
 * @param replyTo actor to send OrderedEvents to
 * @param chunkSize number of events to request at a time
 * @param chunkReceiveTimeout how long to wait for events before stopping
 */
class GapFetcher(firstGapSeq: Long, target: ActorRef, replyTo: ActorRef, chunkSize: Int, chunkReceiveTimeout: Int)
                extends Actor {

  context.setReceiveTimeout(chunkReceiveTimeout seconds)

  requestChunk(firstGapSeq, chunkSize)

  def receive = {
    case logChunk: LogSubrange =>
      replyTo ! logChunk

    case RequestFromSeq(seq: Long) =>
      requestChunk(seq, chunkSize)

    case ReceiveTimeout =>
      context.stop(self)
  }

  private[bridge] def requestChunk(currentGapSeq: Long, chunkSize: Int) {
    target ! GetLogSubrange(currentGapSeq, currentGapSeq + chunkSize-1)
  }

}

