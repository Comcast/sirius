package com.comcast.xfinity.sirius.api.impl.paxos
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import collection.immutable.SortedMap
import java.util.{TreeMap => JTreeMap}
import scala.collection.JavaConversions._

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
  def update[T](x: JTreeMap[Long, T], y: JTreeMap[Long, T]): JTreeMap[Long, T] = {
    val toReturn: JTreeMap[Long, T] = y.clone.asInstanceOf[JTreeMap[Long, T]]
    for (key <- x.keySet) {
      if (!toReturn.containsKey(key)) {
        toReturn.put(key, x.get(key))
      }
    }
    toReturn
  }

  /**
   * Takes a Set of PValues and produces a java TreeMap from slot (Long) to Command, retaining the Command
   * associated with the highest Ballot for each slot.
   *
   * @param pvals
   * @return
   */
  def pmax(pvals: Set[PValue]): JTreeMap[Long, Command] = {
    def updateWithMaxBallot(acc: SortedMap[Long, PValue], pval: PValue) = acc.get(pval.slotNum) match {
      case None  => acc + (pval.slotNum -> pval)
      case Some(PValue(otherBallot, _, _)) if pval.ballot >= otherBallot => acc + (pval.slotNum -> pval)
      case _ => acc
    }

    val maxBallotMap = pvals.foldLeft(SortedMap[Long, PValue]())(updateWithMaxBallot)
    maxBallotMap.foldLeft(new JTreeMap[Long, Command]()) {
      case (acc, (slot, pval)) =>
        acc.put(slot, pval.proposedCommand)
        acc
    }
  }
}
