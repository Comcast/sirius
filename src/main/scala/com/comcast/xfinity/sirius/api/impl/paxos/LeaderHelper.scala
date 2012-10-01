package com.comcast.xfinity.sirius.api.impl.paxos
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import collection.immutable.SortedMap
import com.comcast.xfinity.sirius.util.RichJTreeMap

/**
 * Some helpers for more complex operations on the Leader.  Ideally this
 * stuff would be in a companion object, but because we want to mock it,
 * and Maven doesn't support ScalaMock (yet), we have this.
 */
class LeaderHelper {

  /**
   * Overlays x with all entries from y, with the side effect of modifying x.
   *
   * Returns reference to x- this is an artifact of when this was immutable and more
   * complex, this method should get factored out soon.
   *
   * @param x a RichJTreeMap which is modified in place, being overlayed with all values
   *          from y
   * @param y values to overlay on x
   * @return reference to x, which is mutated
   */
  def update[T](x: RichJTreeMap[Long, T], y: RichJTreeMap[Long, T]): RichJTreeMap[Long, T] = {
    x.putAll(y)
    x
  }

  /**
   * Takes a Set of PValues and produces a java TreeMap from slot (Long) to Command, retaining the Command
   * associated with the highest Ballot for each slot.
   *
   * @param pvals
   * @return
   */
  def pmax(pvals: Set[PValue]): RichJTreeMap[Long, Command] = {
    def updateWithMaxBallot(acc: SortedMap[Long, PValue], pval: PValue) = acc.get(pval.slotNum) match {
      case None  => acc + (pval.slotNum -> pval)
      case Some(PValue(otherBallot, _, _)) if pval.ballot >= otherBallot => acc + (pval.slotNum -> pval)
      case _ => acc
    }

    val maxBallotMap = pvals.foldLeft(SortedMap[Long, PValue]())(updateWithMaxBallot)
    maxBallotMap.foldLeft(new RichJTreeMap[Long, Command]()) {
      case (acc, (slot, pval)) =>
        acc.put(slot, pval.proposedCommand)
        acc
    }
  }
}
