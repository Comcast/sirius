package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{Props, Actor, ActorRef}
import akka.agent.Agent

object Leader {
  def proposalExistsForSlot(proposals: Set[Slot], newSlotNum: Int) = proposals.exists {
    case Slot(existingSlotNum, _) if existingSlotNum == newSlotNum => true
    case _ => false
  }

  //TODO: Isn't this just set union?
  def update[T](x: Set[T], y: Set[T]) = y ++ x.filterNot(y.contains(_))

  def pmax(pvals: Set[PValue]): Set[Slot] = {
    def updateWithMaxBallot(acc: Map[Int, PValue], pval: PValue) = acc.get(pval.slotNum) match {
      case None  => acc + (pval.slotNum -> pval)
      case Some(PValue(otherBallot, _, _)) if pval.ballot >= otherBallot => acc + (pval.slotNum -> pval)
      case _ => acc
    }

    val maxBallotMap = pvals.foldLeft(Map[Int, PValue]())(updateWithMaxBallot)
    maxBallotMap.foldLeft(Set[Slot]()) {
      case (acc, (_, PValue(_, slotNum, proposal))) => acc + Slot(slotNum, proposal)
    }
  }
}

class Leader(membership: Agent[Set[ActorRef]]) extends Actor {

  import Leader._

  val acceptors = membership
  val replicas = membership

  var ballotNum = Ballot(0, self.toString)
  var active = false
  var proposals = Set[Slot]()

  context.actorOf(Props(new Scout(self, acceptors(), ballotNum)))

  def receive = {
    case Propose(slotNum, command) =>
      if (!proposalExistsForSlot(proposals, slotNum)) {
        proposals += Slot(slotNum, command)
        if (active) {
          startCommander(PValue(ballotNum, slotNum, command))
        }
      }
    case Adopted(newBallotNum, pvals) if ballotNum == newBallotNum =>
      proposals = update(proposals, pmax(pvals))
      proposals.foreach(slot =>
        startCommander(PValue(ballotNum, slot.num, slot.command))
      )
      active = true
      println(self + " became active with ballot " + ballotNum)
    case Preempted(newBallot) =>
      println(self + " preempted with " + newBallot + " from " + ballotNum)
      if (newBallot > ballotNum) {
        println(self + " really preempted with " + newBallot)
        active = false
        ballotNum = Ballot(newBallot.seq + 1, self.toString)
        startScout()
      }

    // if our scout fails to make progress, retry
    case ScoutTimeout =>
      context.actorOf(Props(new Scout(self, acceptors(), ballotNum)))
  }

  private def startCommander(pval: PValue) {
    // XXX: more members may show up between when acceptors() and replicas(),
    //      we may want to combine the two, and just reference membership
    context.actorOf(Props(new Commander(self, acceptors(), replicas(), pval)))
  }

  private def startScout() {
    context.actorOf(Props(new Scout(self, acceptors(), ballotNum)))
  }

}