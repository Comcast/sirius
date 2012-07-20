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

  case class Command(k: ActorRef, ts: Long, op: NonCommutativeSiriusRequest) extends PaxosMessage

  case class PValue(ballot: Ballot, slotNum: Long, proposedCommand: Command) extends PaxosMessage

  case class Phase1A(from: ActorRef, ballot: Ballot) extends PaxosMessage

  case class Phase1B(from: ActorRef, ballot: Ballot, r: Set[PValue]) extends PaxosMessage

  case class Phase2A(from: ActorRef, pvalue: PValue) extends PaxosMessage

  case class Phase2B(acceptor: ActorRef, ballot: Ballot) extends PaxosMessage

  case class Slot(num: Long, command: Command) extends PaxosMessage

  case object ScoutTimeout extends PaxosMessage

}