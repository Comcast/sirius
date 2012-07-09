package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor._

object PaxosMessages {

  case class Propose(slot: Int, command: Command)
  case class Adopted(ballotNum: Ballot, pvals: Set[PValue])
  case class Preempted(picked: Ballot)

  case class Request(command: Command)
  case class Decision(slot: Int, command: Command)

  case class Command(k: ActorRef, cid: Int, op: Int)
  case class PValue(ballot: Ballot, slotNum: Int, proposal: Command)

  case class Phase1A(from: ActorRef, ballot: Ballot)
  case class Phase1B(from: ActorRef, ballot: Ballot, r: Set[PValue])
  case class Phase2A(from: ActorRef, pvalue: PValue)
  case class Phase2B(acceptor: ActorRef, ballot: Ballot)

  case class Slot(num: Int, command: Command)

  case class Ballot(seq: Int, leaderId: String) extends Ordered[Ballot] {
    def compare(that: Ballot) = that match {
      case Ballot(thatSeq, _) if seq < thatSeq => -1
      case Ballot(thatSeq, _) if seq > thatSeq => 1
      case Ballot(_, thatLeaderId) if leaderId < thatLeaderId => -1
      case Ballot(_, thatLeaderId) if leaderId > thatLeaderId => 1
      case _ => 0
    }
  }

  case object ScoutTimeout

  object Ballot {
    val empty = Ballot(Int.MinValue, "")
  }
}