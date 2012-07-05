package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.agent.Agent


object Replica {
  trait HelperProvider {
    val replicaHelper: ReplicaHelper
  }

  def apply(membership: Agent[Set[ActorRef]]): Replica = {
    new Replica(membership) with HelperProvider {
      val replicaHelper = new ReplicaHelper()
    }
  }
}

/**
 * TODO:
 * Must persist slotNum, lowestUnusedSlot to disk and read them in on initialization.
 * Must persist proposals and decisions to disk.
 * initialState is actually just an ActorRef to the State actor.
 */
class Replica(membership: Agent[Set[ActorRef]]) extends Actor {
    this: Replica.HelperProvider =>
  //var state = initialState

  val leaders = membership

  var highestPerformedSlot = 0

  var proposals = Set[Slot]()
  var decisions = Set[Slot]()

  def propose(command: Command) {
    if (!replicaHelper.decisionExistsForCommand(decisions, command)) {
      val lowestUnusedSlotNum = replicaHelper.getLowestUnusedSlotNum(proposals ++ decisions)
      proposals += Slot(lowestUnusedSlotNum, command)
      // TODO: only route to our leader?
      leaders().foreach(_ ! Propose(lowestUnusedSlotNum, command))
    }
  }

  def perform(command: Command) {
    // send command to state actor.
    highestPerformedSlot += 1
    // respond to client
  }

  def receive = {
    case Request(command: Command) => propose(command)
    case Decision(slot, command) =>
      println(self + " received Decision(" + slot + ", " + command + ") from " + sender)
      decisions += Slot(slot, command)

      // XXX: is foreach order gaurenteed?
      val unperformedDecisions = replicaHelper.getUnperformedDecisions(decisions, highestPerformedSlot)
      unperformedDecisions.foreach (slot => {
        println(self + " " + slot.num + " " + slot.command)
        //highestPerformedSlot = slot.num
        perform(slot.command)
      })
  }
}