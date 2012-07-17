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

  // TODO: we don't actually use cid, we can commandeer that for a retry
  // field when we get to that point to avoid having to change a ton of code,
  // though that will make comparison weird
  case class Command(k: ActorRef, cid: Int, op: NonCommutativeSiriusRequest) extends PaxosMessage

  case class PValue(ballot: Ballot, slotNum: Long, proposal: Command) extends PaxosMessage

  case class Phase1A(from: ActorRef, ballot: Ballot) extends PaxosMessage

  case class Phase1B(from: ActorRef, ballot: Ballot, r: Set[PValue]) extends PaxosMessage

  case class Phase2A(from: ActorRef, pvalue: PValue) extends PaxosMessage

  case class Phase2B(acceptor: ActorRef, ballot: Ballot) extends PaxosMessage

  case class Slot(num: Long, command: Command) extends PaxosMessage

  case object ScoutTimeout extends PaxosMessage

}