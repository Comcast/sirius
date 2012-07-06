package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._


object ReplicaActor {
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

/**
 * TODO:
 * Must persist slotNum, lowestUnusedSlot to disk and read them in on initialization.
 * Must persist proposals and decisions to disk.
 * initialState is actually just an ActorRef to the State actor.
 */
class ReplicaActor(leaders: Set[ActorRef]) extends Actor {
  //var state = initialState

  import ReplicaActor._

  var highestPerformedSlot = 0

  var proposals = Set[Slot]()
  var decisions = Set[Slot]()

  def propose(command: Command) = {
    if (!decisionExistsForCommand(decisions, command)) {
      val lowestUnusedSlotNum = getLowestUnusedSlotNum(proposals ++ decisions)
      proposals += Slot(lowestUnusedSlotNum, command)
      // TODO: only route to our leader?
      leaders.foreach(_ ! Propose(lowestUnusedSlotNum, command))
    }
  }

  def perform(command: Command) = {
    // send command to state actor.
    highestPerformedSlot += 1
    // respond to client
  }

  def receive = {
    case Request(command: Command) => propose(command)
    case Decision(slot, command) =>
      decisions += Slot(slot, command)

      // XXX: is foreach order gaurenteed?
      getUnperformedDecisions(decisions, highestPerformedSlot).foreach(println(_))
  }
}