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

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{ActorContext, Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest
import akka.event.Logging
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosSupervisor.ChildProvider
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper

object PaxosSupervisor {

  /**
   * Factory for creating children actors of PaxosSup.
   *
   * @param membership an [[ akka.agent.Agent]] tracking the membership of the cluster
   * @param startingSeq the sequence number at which this node will begin issuing/acknowledging
   * @param performFun function specified by
   *          [[com.comcast.xfinity.sirius.api.impl.paxos.Replica.PerformFun]], applied to
   *          decisions as they arrive
   * @param config SiriusConfiguration object for configuring children actors.
   */
  protected[paxos] class ChildProvider(membership: MembershipHelper, startingSeq: Long, performFun: Replica.PerformFun, config: SiriusConfiguration) {
    def createLeader()(implicit context: ActorContext) =
      context.actorOf(Leader.props(membership, startingSeq, config), "leader")

    def createAcceptor()(implicit context: ActorContext) =
      context.actorOf(Acceptor.props(startingSeq, config), "acceptor")

    def createReplica(leader: ActorRef)(implicit context: ActorContext) =
      context.actorOf(Replica.props(leader, startingSeq, performFun, config), "replica")
  }

  /**
   * Create Props for a PaxosSupervisor actor.
   *
   * @param membership
   * @param startingSeqNum the sequence number at which this node will begin issuing/acknowledging
   * @param performFun function specified by
   *          [[com.comcast.xfinity.sirius.api.impl.paxos.Replica.PerformFun]], applied to
   *          decisions as they arrive
   * @param config SiriusConfiguration for this node
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(membership: MembershipHelper,
            startingSeqNum: Long,
            performFun: Replica.PerformFun,
            config: SiriusConfiguration): Props = {
     Props(classOf[PaxosSupervisor], new ChildProvider(membership, startingSeqNum, performFun, config))
  }

}

class PaxosSupervisor(childProvider: ChildProvider) extends Actor {

  val leader = childProvider.createLeader
  val acceptor = childProvider.createAcceptor
  val replica = childProvider.createReplica(leader)

  val traceLog = Logging(context.system, "SiriusTrace")

  def receive = {
    // Replica messages
    case req: NonCommutativeSiriusRequest =>
      traceLog.debug("Received event for submission {}", req)
      val command = Command(sender, System.currentTimeMillis(), req)
      replica forward Request(command)
    case d: Decision => replica forward d
    case dh: DecisionHint => replica forward  dh

    // Leader messages
    case p: Propose => leader forward p
    // Adopted and Preempted are internal
    // Acceptor messages
    case p1a: Phase1A => acceptor forward p1a
    case p2A: Phase2A => acceptor forward p2A
    // Phase1B and Phase2B are direct addressed
  }
}
