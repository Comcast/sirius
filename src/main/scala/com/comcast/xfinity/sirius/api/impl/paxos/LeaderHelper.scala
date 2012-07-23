package com.comcast.xfinity.sirius.api.impl.paxos
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import collection.immutable.SortedMap

/**
 * Some helpers for more complex operations on the Leader.  Ideally this
 * stuff would be in a companion object, but because we want to mock it,
 * and Maven doesn't support ScalaMock (yet), we have this.
 */
class LeaderHelper {

  /**
   * Takes the union of all key/values in y and all key/values in x such that no value
   * exists for the key in y.
   *
   * @param x
   * @param y
   * @return
   */
  def update[T](x: SortedMap[Long, T], y: SortedMap[Long, T]): SortedMap[Long, T] =
    x.foldLeft(y) {
      case (acc, (slot, _)) if acc.contains(slot) => acc
      case (acc, kv) => acc + kv
    }


  /**
   * Takes a Set of PValues and produces a SortedMap from slot (Long) to Command, retaining the Command
   * associated with the highest Ballot for each slot.
   *
   * @param pvals
   * @return
   */
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
