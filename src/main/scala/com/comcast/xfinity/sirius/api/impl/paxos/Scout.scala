package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.{Actor, ActorRef}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._


class Scout(leader: ActorRef, acceptors: Set[ActorRef], ballot: Ballot) extends Actor {
  var waitFor = acceptors
  var pvalues = Set[PValue]()

  acceptors.foreach(_ ! Phase1A(self, ballot))

  def receive = {
    case Phase1B(acceptor, theirBallot, theirPvals) =>
      if (theirBallot == ballot) {
        pvalues ++= theirPvals
        waitFor -= acceptor
        if (waitFor.size < acceptors.size / 2) {
          leader ! Adopted(ballot, pvalues)
          context.stop(self)
        }
      } else {
        leader ! Preempted(theirBallot)
        context.stop(self)
      }
  }
}