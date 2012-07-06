package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.{Actor, ActorRef}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

class Commander(leader: ActorRef, acceptors: Set[ActorRef],
                replicas: Set[ActorRef], pval: PValue) extends Actor {

  var waitFor = acceptors

  acceptors.foreach(_ ! Phase2A(self, pval))

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
      }
    }
}