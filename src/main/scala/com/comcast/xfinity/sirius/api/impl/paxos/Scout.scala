package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.util.duration._
import akka.actor.{ReceiveTimeout, Actor, ActorRef}

class Scout(leader: ActorRef, acceptors: Set[ActorRef], ballot: Ballot, latestDecidedSlot: Long) extends Actor {

  var decidedAcceptors = Set[ActorRef]()
  var pvalues = Set[PValue]()

  acceptors.foreach(
    node => node ! Phase1A(self, ballot, node, latestDecidedSlot)
  )

  context.setReceiveTimeout(3 seconds)

  def receive = {
    case Phase1B(acceptor, theirBallot, theirPvals) if theirBallot == ballot =>
      pvalues ++= theirPvals
      if (acceptors.contains(acceptor)) {
        decidedAcceptors += acceptor
      }
      if (decidedAcceptors.size > acceptors.size / 2) {
        leader ! Adopted(ballot, pvalues)
        context.stop(self)
      }
    // Per Paxos Made Moderately Complex, acceptedBallot MUST be greater than our own here
    case Phase1B(acceptor, theirBallot, theirPvals) =>
        leader ! Preempted(theirBallot)
        context.stop(self)

    case ReceiveTimeout =>
      leader ! ScoutTimeout
      context.stop(self)
  }
}