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
    case Phase2B(acceptor, acceptedBallot) =>
      if (pval.ballot == acceptedBallot) {
        waitFor -= acceptor
        if (waitFor.size < acceptors.size / 2) {
          replicas.foreach(_ ! Decision(pval.slotNum, pval.proposal))
          context.stop(self)
        }
      } else {
        leader ! Preempted(acceptedBallot)
        context.stop(self)
      }
    case ReceiveTimeout =>
      context.stop(self)
  }
}