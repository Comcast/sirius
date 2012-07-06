package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor.ActorRef
import akka.actor.Actor
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

class Acceptor extends Actor {
  var ballotNum: Ballot = Ballot.empty
  var accepted = Set[PValue]()

  def receive = {
    case Phase1A(leader, ballot) =>
      if (ballot > ballotNum) ballotNum = ballot
      leader ! Phase1B(self, ballotNum, accepted)
    case Phase2A(leader, PValue(ballot, slot, command)) =>
      if (ballot >= ballotNum) {
        ballotNum = ballot
        accepted += PValue(ballot, slot, command)
      }
      leader ! Phase2B(self, ballotNum)
  }
}