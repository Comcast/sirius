package com.comcast.xfinity.sirius.api.impl.paxos
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

class LeaderHelper {
  def proposalExistsForSlot(proposals: Set[Slot], newSlotNum: Long) = proposals.exists {
    case Slot(existingSlotNum, _) if existingSlotNum == newSlotNum => true
    case _ => false
  }

  def update(x: Set[Slot], y: Set[Slot]) = {
    val slotsInXNotInY = x.filterNot(slot => y.exists {
      case Slot(slotNum, _) if slotNum == slot.num => true
      case _ => false
    })
    y ++ slotsInXNotInY
  }

  def pmax(pvals: Set[PValue]): Set[Slot] = {
    def updateWithMaxBallot(acc: Map[Long, PValue], pval: PValue) = acc.get(pval.slotNum) match {
      case None  => acc + (pval.slotNum -> pval)
      case Some(PValue(otherBallot, _, _)) if pval.ballot >= otherBallot => acc + (pval.slotNum -> pval)
      case _ => acc
    }

    val maxBallotMap = pvals.foldLeft(Map[Long, PValue]())(updateWithMaxBallot)
    maxBallotMap.foldLeft(Set[Slot]()) {
      case (acc, (_, PValue(_, slotNum, proposal))) => acc + Slot(slotNum, proposal)
    }
  }
}