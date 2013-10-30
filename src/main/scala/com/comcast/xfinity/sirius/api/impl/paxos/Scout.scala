package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.util.duration._
import akka.actor.{Props, ReceiveTimeout, Actor, ActorRef}
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper.ClusterInfo

object Scout{

  /**
   * Create Props for a Scout actor.
   *
   * @param leader actorRef of local leader
   * @param clusterInfo current cluster membership information
   * @param ballot ballot to try and get accepted
   * @param latestDecidedSlot latest locally decided slot
   *
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(leader: ActorRef, clusterInfo: ClusterInfo, ballot: Ballot, latestDecidedSlot: Long): Props = {
    //Props(classOf[Scout], leader, clusterInfo, ballot, latestDecidedSlot)
    Props(new Scout(leader, clusterInfo.activeMembers, ballot, latestDecidedSlot, clusterInfo.simpleMajority))
  }
}

class Scout(leader: ActorRef, acceptors: Set[ActorRef], ballot: Ballot, latestDecidedSlot: Long, simpleMajority: Int) extends Actor {

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
      if (decidedAcceptors.size >= simpleMajority) {
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
