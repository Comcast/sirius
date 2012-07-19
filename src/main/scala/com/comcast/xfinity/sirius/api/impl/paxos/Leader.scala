package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{ Props, Actor, ActorRef }
import akka.agent.Agent

object Leader {
  trait HelperProvider {
    val leaderHelper: LeaderHelper
    def startCommander(pval: PValue): Unit
    def startScout(): Unit
  }

  def apply(membership: Agent[Set[ActorRef]]): Leader = {
    new Leader(membership) with HelperProvider {
      val leaderHelper = new LeaderHelper()

      def startCommander(pval: PValue) {
        // XXX: more members may show up between when acceptors() and replicas(),
        //      we may want to combine the two, and just reference membership
        context.actorOf(Props(new Commander(self, acceptors(), replicas(), pval)))
      }

      def startScout() {
        context.actorOf(Props(new Scout(self, acceptors(), ballotNum)))
      }
    }
  }
}

class Leader(membership: Agent[Set[ActorRef]]) extends Actor {
  this: Leader.HelperProvider =>

  val acceptors = membership
  val replicas = membership

  var ballotNum = Ballot(0, self.toString)
  var active = false
  var proposals = Map[Long, Command]()

  startScout()

  def receive = {
    case Propose(slotNum, command) if !proposals.contains(slotNum) =>
      proposals += (slotNum -> command)
      if (active) {
        startCommander(PValue(ballotNum, slotNum, command))
      }

    case Adopted(newBallotNum, pvals) if ballotNum == newBallotNum =>
      proposals = leaderHelper.update(proposals, leaderHelper.pmax(pvals))
      proposals.foreach {
        case (slot, command) => startCommander(PValue(ballotNum, slot, command))
      }
      active = true

    case Preempted(newBallot) if newBallot > ballotNum =>
      active = false
      ballotNum = Ballot(newBallot.seq + 1, self.toString)
      startScout()

    // if our scout fails to make progress, retry
    case ScoutTimeout => startScout()
  }
}