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

import akka.actor._
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest

object PaxosMessages {

  sealed trait PaxosMessage

  case class Propose(slot: Long, command: Command) extends PaxosMessage

  case class Adopted(ballotNum: Ballot, pvals: Set[PValue]) extends PaxosMessage

  case class Preempted(picked: Ballot) extends PaxosMessage

  case class Request(command: Command) extends PaxosMessage

  case class Decision(slot: Long, command: Command) extends PaxosMessage

  /**
   * A Hint about the last Decision executed by the Sirius System.  Used to reduce
   * the amount of information the Acceptor sends in Phase1B message.  Also used by
   * the Leader to not repropose already decided proposals.
   *
   * @param decision last decision executed
   */
  case class DecisionHint(decision: Long) extends PaxosMessage

  case class Command(client: ActorRef, ts: Long, op: NonCommutativeSiriusRequest) extends PaxosMessage

  case class PValue(ballot: Ballot, slotNum: Long, proposedCommand: Command) extends PaxosMessage

  /**
   * Message used by a Scout to advocate a Ballot
   *
   * @param from The ActorRef sending this request, this will always be the sending Scout,
   *            and most likely always be the same as the sender of this message.  We
   *            include this param for similarity to the algorithm described in Paxos Made
   *            Moderately Complex and for flexibility
   * @param ballot The Ballot which we are trying to make active
   * @param replyAs An ActorRef that the receiving acceptor must supply in the from field of
   *            the Phase1B message, allowing us to cleanly uniquely identify a node
   * @param latestDecidedSlot The lastest decided slot that we know about, used to reduce number of
   *            accepted decisions sent in Phase1B
   * @see DecisionHint
   */
  case class Phase1A(from: ActorRef, ballot: Ballot, replyAs: ActorRef, latestDecidedSlot: Long) extends PaxosMessage

  /**
   * Message sent back to a Scout from an Acceptor during Ballot negotiation
   *
   * @param from The ActorRef identifying the sending acceptor, this should always be set to
   *            the value supplied in the replyAs field of Phase1A messages, such that the
   *            requesting Scout can properly identify the source of the message
   * @param ballot The sending Acceptor's current Ballot
   * @param r the set of PValues this Acceptor has retained TODO: this name could be better
   */
  case class Phase1B(from: ActorRef, ballot: Ballot, r: Set[PValue]) extends PaxosMessage

  /**
   * Message used by commander to advocate a PValue (used to find a decision for a Proposal)
   *
   * @param from The ActorRef sending this request, this will always be the sending Commander,
   *            and most likely always be the same as the sender of this message.  We
   *            include this param for similarity to the algorithm described in Paxos Made
   *            Moderately Complex and for flexibility
   * @param pvalue The PValue for which the initiating commander is trying to have the cluster
   *            arrive at a decision
   * @param replyAs An ActorRef that the receiving acceptor must supply in the from field of
   *            the Phase2B message, allowing us to cleanly uniquely identify a node
   */
  case class Phase2A(from: ActorRef, pvalue: PValue, replyAs: ActorRef) extends PaxosMessage

  /**
   * Message sent back to a Commander from an Acceptor when deciding on a PValue
   *
   * @param acceptor The ActorRef identifying the sending acceptor, this should always be set to
   *            the value supplied in the replyAs field of Phase2A messages, such that the
   *            requesting Scout can properly identify the source of the message
   * @param ballot The sending Acceptor's current Ballot
   */
  case class Phase2B(acceptor: ActorRef, ballot: Ballot) extends PaxosMessage

  case class Slot(num: Long, command: Command) extends PaxosMessage

  case object ScoutTimeout extends PaxosMessage

}
