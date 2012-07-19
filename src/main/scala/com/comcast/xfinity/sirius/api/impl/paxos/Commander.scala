package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.util.duration._
import akka.actor.{ReceiveTimeout, Actor, ActorRef}

class Commander(leader: ActorRef, acceptors: Set[ActorRef],
                replicas: Set[ActorRef], pval: PValue) extends Actor {

  var waitFor = acceptors

  acceptors.foreach(_ ! Phase2A(self, pval))

  context.setReceiveTimeout(3 seconds)

  def receive = {
    case Phase2B(acceptor, acceptedBallot) if acceptedBallot == pval.ballot =>
      waitFor -= acceptor
      if (waitFor.size < acceptors.size / 2) {
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
      context.stop(self)
  }
}