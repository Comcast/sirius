package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.agent.Agent

object Replica {
  def apply(membership: Agent[Set[ActorRef]]): Replica = new Replica(membership)
}

/**
 * TODO:
 * Must persist slotNum, lowestUnusedSlot to disk and read them in on initialization.
 * Must persist proposals and decisions to disk.
 * initialState is actually just an ActorRef to the State actor.
 */
class Replica(membership: Agent[Set[ActorRef]]) extends Actor {
  val leaders = membership
  var lowestUnusedSlotNum : Long = 1

  var proposals = Set[Slot]()

  def propose(command: Command) {
    proposals += Slot(lowestUnusedSlotNum, command)
    leaders().foreach(_ ! Propose(lowestUnusedSlotNum, command))
    lowestUnusedSlotNum = lowestUnusedSlotNum + 1
  }

  def receive = {
    case Request(command: Command) => propose(command)
    case Decision(slot, command) =>
      if (slot >= lowestUnusedSlotNum) lowestUnusedSlotNum = slot + 1
    //perform decision
    //reply to client
  }
}