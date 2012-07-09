package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor.ActorRef
import akka.actor.Actor
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

class Acceptor extends Actor {
  var ballotNum: Ballot = Ballot.empty
  var accepted = Set[PValue]()

  // Note that Phase1A and Phase2B requests must return their
  // parent's address (their PaxosSup) because this is how
  // the acceptor is known to external nodes
  def receive = {
    case Phase1A(scout, ballot) =>
      if (ballot > ballotNum) ballotNum = ballot
      scout ! Phase1B(context.parent, ballotNum, accepted)
    case Phase2A(commander, PValue(ballot, slot, command)) =>
      if (ballot >= ballotNum) {
        ballotNum = ballot
        accepted += PValue(ballot, slot, command)
      }
      commander ! Phase2B(context.parent, ballotNum)
  }
}