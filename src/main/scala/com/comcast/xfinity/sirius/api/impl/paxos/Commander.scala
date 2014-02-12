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
import scala.concurrent.duration._
import akka.actor.{Props, ReceiveTimeout, Actor, ActorRef}
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper.ClusterInfo
import scala.language.postfixOps

case object Commander {

  /**
   * Message sent by a Commander to its leader when it times out
   */
  case class CommanderTimeout(pval: PValue, retriesLeft: Int)

  /**
   * Create Props for a Commander actor.
   *
   * @param leader actorRef of local leader
   * @param clusterInfo current cluster membership information
   * @param pval PValue to try and win acceptance
   * @param retriesLeft number of retries left for getting pval accepted
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(leader: ActorRef, clusterInfo: ClusterInfo, pval: PValue, retriesLeft: Int): Props = {
    Props(classOf[Commander], leader, clusterInfo.activeMembers, clusterInfo.activeMembers, pval, clusterInfo.simpleMajority, retriesLeft)
  }
}

class Commander(leader: ActorRef, acceptors: Set[ActorRef], replicas: Set[ActorRef],
                pval: PValue, simpleMajority: Int, retriesLeft: Int) extends Actor {

  var decidedAcceptors = Set[ActorRef]()

  acceptors.foreach(
    node => node ! Phase2A(self, pval, node)
  )

  context.setReceiveTimeout(3 seconds)

  def receive = {
    case Phase2B(acceptor, acceptedBallot) if acceptedBallot == pval.ballot =>
      if (acceptors.contains(acceptor)) {
        decidedAcceptors += acceptor
      }
      if (decidedAcceptors.size >= simpleMajority) {
        // We may fail to send a decision message to a replica, and we need to handle
        // that in our catchup algorithm.  The paxos made moderately complex algorithm
        // assumes guaranteed delivery, because its cool like that.
        replicas.foreach(_ ! Decision(pval.slotNum, pval.proposedCommand))
        context.stop(self)
      }

    // Per Paxos Made Moderately Complex, acceptedBallot MUST be greater than our own here
    case Phase2B(acceptor, acceptedBallot) =>
      leader ! Preempted(acceptedBallot)
      context.stop(self)

    case ReceiveTimeout =>
      leader ! Commander.CommanderTimeout(pval, retriesLeft)
      context.stop(self)
  }
}
