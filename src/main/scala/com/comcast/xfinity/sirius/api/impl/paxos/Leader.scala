package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{Props, Actor, ActorRef}

object Leader {
  def proposalExistsForSlot(proposals: Set[Slot], newSlotNum: Int) = proposals.exists {
    case Slot(existingSlotNum, _) if existingSlotNum == newSlotNum => true
    case _ => false
  }

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

class Leader(acceptors: Set[ActorRef], replicas: Set[ActorRef]) extends Actor {

  import Leader._

  var ballotNum = Ballot(0, self.toString)
  var active = false
  var proposals = Set[Slot]()

  context.actorOf(Props(new Scout(self, acceptors, ballotNum)))

  def receive = {
    case Propose(slotNum, command) =>
      if (!proposalExistsForSlot(proposals, slotNum)) {
        proposals += Slot(slotNum, command)
        if (active) {
          startCommander(PValue(ballotNum, slotNum, command))
        }
      }
    case Adopted(ballotNum, pvals) =>
      proposals = update(proposals, pmax(pvals))
      proposals.foreach(slot =>
        startCommander(PValue(ballotNum, slot.num, slot.command))
      )
      active = true
    case Preempted(newBallot) =>
      if (newBallot > ballotNum) {
        active = false
        ballotNum = Ballot(newBallot.seq + 1, self.toString)
        startScout()
      }
  }

  private def startCommander(pval: PValue) {
    context.actorOf(Props(new Commander(self, acceptors, replicas, pval)))
  }

  private def startScout() {
    context.actorOf(Props(new Scout(self, acceptors, ballotNum)))
  }

}