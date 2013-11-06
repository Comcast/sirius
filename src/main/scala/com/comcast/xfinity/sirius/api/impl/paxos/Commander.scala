package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.util.duration._
import akka.actor.{Props, ReceiveTimeout, Actor, ActorRef}

case object Commander {

  /**
   * Message sent by a Commander to its leader when it times out
   */
  case class CommanderTimeout(pval: PValue, retriesLeft: Int)

  /**
   * Create Props for a Commander actor.
   *
   * @param leader actorRef of local leader
   * @param acceptors set of remote Acceptors
   * @param replicas set of remote Replicas
   * @param pval PValue to try and win acceptance
   * @param retriesLeft number of retries left for getting pval accepted
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(leader: ActorRef, acceptors: Set[ActorRef],
            replicas: Set[ActorRef], pval: PValue, retriesLeft: Int): Props = {
    //Props(classOf[Commander], leader, acceptors, replicas, pval, retriesLeft)
    Props(new Commander(leader, acceptors, replicas, pval, retriesLeft))
  }
}

class Commander(leader: ActorRef, acceptors: Set[ActorRef],
                replicas: Set[ActorRef], pval: PValue, retriesLeft: Int) extends Actor {

  var decidedAcceptors = Set[ActorRef]()

  acceptors.foreach(
    node => node ! Phase2A(self, pval, node)
  )

  context.setReceiveTimeout(3 seconds)

  def receive = {
    case Phase2B(acceptor, acceptedBallot) if acceptedBallot == pval.ballot =>
      if (acceptors.contains(acceptor)) {
        decidedAcceptors += acceptor
      }
      if (decidedAcceptors.size > acceptors.size / 2) {
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
      leader ! Commander.CommanderTimeout(pval, retriesLeft)
      context.stop(self)
  }
}