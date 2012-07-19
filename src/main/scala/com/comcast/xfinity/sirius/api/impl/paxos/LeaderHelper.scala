package com.comcast.xfinity.sirius.api.impl.paxos
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

class LeaderHelper {

  def update[T](x: Map[Long, T], y: Map[Long, T]): Map[Long, T] =
    x.foldLeft(y) {
      case (acc, (slot, _)) if acc.contains(slot) => acc
      case (acc, kv) => acc + kv
    }

  def pmax(pvals: Set[PValue]): Map[Long, Command] = {
    def updateWithMaxBallot(acc: Map[Long, PValue], pval: PValue) = acc.get(pval.slotNum) match {
      case None  => acc + (pval.slotNum -> pval)
      case Some(PValue(otherBallot, _, _)) if pval.ballot >= otherBallot => acc + (pval.slotNum -> pval)
      case _ => acc
    }

    val maxBallotMap = pvals.foldLeft(Map[Long, PValue]())(updateWithMaxBallot)
    maxBallotMap.mapValues(pval => pval.proposedCommand)
  }

}