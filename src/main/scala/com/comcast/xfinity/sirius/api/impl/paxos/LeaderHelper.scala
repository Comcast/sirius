package com.comcast.xfinity.sirius.api.impl.paxos
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import collection.immutable.SortedMap

class LeaderHelper {

  def update[T](x: SortedMap[Long, T], y: SortedMap[Long, T]): SortedMap[Long, T] =
    x.foldLeft(y) {
      case (acc, (slot, _)) if acc.contains(slot) => acc
      case (acc, kv) => acc + kv
    }

  def pmax(pvals: Set[PValue]): SortedMap[Long, Command] = {
    def updateWithMaxBallot(acc: SortedMap[Long, PValue], pval: PValue) = acc.get(pval.slotNum) match {
      case None  => acc + (pval.slotNum -> pval)
      case Some(PValue(otherBallot, _, _)) if pval.ballot >= otherBallot => acc + (pval.slotNum -> pval)
      case _ => acc
    }

    val maxBallotMap = pvals.foldLeft(SortedMap[Long, PValue]())(updateWithMaxBallot)
    maxBallotMap.foldLeft(SortedMap[Long, Command]()) {
      case (acc, (slot, pval)) => acc + (slot -> pval.proposedCommand)
    }
  }

}
