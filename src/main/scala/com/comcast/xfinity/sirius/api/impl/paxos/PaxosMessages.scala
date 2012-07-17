package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor._
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest

object PaxosMessages {

  case class Propose(slot: Long, command: Command)
  case class Adopted(ballotNum: Ballot, pvals: Set[PValue])
  case class Preempted(picked: Ballot)

  case class Request(command: Command)
  case class Decision(slot: Long, command: Command)

  //XXX: Need to figure out what op actually is, cuz it isn't an int.
  //Need to ensure that we can compare commands since the algorithm
  //requires it, though we could punt on it and just rely on the 
  //fact that commands are idempotent.
  case class Command(k: ActorRef, cid: Int, op: NonCommutativeSiriusRequest)
  case class PValue(ballot: Ballot, slotNum: Long, proposal: Command)

  case class Phase1A(from: ActorRef, ballot: Ballot)
  case class Phase1B(from: ActorRef, ballot: Ballot, r: Set[PValue])
  case class Phase2A(from: ActorRef, pvalue: PValue)
  case class Phase2B(acceptor: ActorRef, ballot: Ballot)

  case class Slot(num: Long, command: Command)

  case object ScoutTimeout
}