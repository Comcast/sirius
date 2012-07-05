package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.{Command, Slot}

/**
 * Utility methods for the more complex logic of a replica.
 *
 * Note: This code is better off in a companion object, however, due to the limitations
 * of Maven, and its lack of scalamock support, it would make testing difficult
 */
class ReplicaHelper {

  def decisionExistsForCommand(decisions: Set[Slot], command: Command): Boolean = decisions.exists {
    case Slot(_, `command`) => true
    case _ => false
  }

  def getLowestUnusedSlotNum(slots: Set[Slot]): Int = slots.foldLeft(0) {
    case (highestSlotNum, Slot(num, _)) if num > highestSlotNum => num
    case (highestSlotNum, _) => highestSlotNum
  } + 1

  def getUnperformedDecisions(decisions: Set[Slot], highestPerformedSlotNum: Int): List[Slot] = {
    val unperformedDecisions = decisions.filter(_.num > highestPerformedSlotNum).toList.sortWith(_.num < _.num)

    def findContiguousDecisions(currentSlotNum: Int, pendingDecisions: List[Slot],
                              acc: List[Slot]): List[Slot] = pendingDecisions match {
      case Nil => acc.reverse
      case hd :: tl if hd.num == currentSlotNum => findContiguousDecisions(currentSlotNum + 1, tl, hd :: acc)
      case _ => acc.reverse
    }

    findContiguousDecisions(highestPerformedSlotNum + 1, unperformedDecisions, Nil)
  }
}