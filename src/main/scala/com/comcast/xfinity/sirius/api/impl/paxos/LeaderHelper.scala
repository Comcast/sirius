package com.comcast.xfinity.sirius.api.impl.paxos
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

class LeaderHelper {
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