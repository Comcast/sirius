package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest
import akka.event.Logging

object Replica {

  type PerformFun = (Long, NonCommutativeSiriusRequest) => Unit

  /**
   * Create a Replica instance.  Note this should be called from within a Props
   * factory on Actor creation due to the requirements of Akka.
   *
   * @param membership an {@link akka.agent.Agent} tracking the membership of the cluster
   * @param performFun function specified by
   *          [[com.comcast.xfinity.sirius.api.impl.paxos.Replica.PerformFun]], applied to
   *          decisions as they arrive
   */
  def apply(membership: Agent[Set[ActorRef]], performFun: PerformFun): Replica =
    new Replica(membership, performFun)
}

/**
 * TODO:
 * Must persist slotNum, lowestUnusedSlot to disk and read them in on initialization.
 * Must persist proposals and decisions to disk.
 * initialState is actually just an ActorRef to the State actor.
 * Keep track of old proposals and re-propose them, if we need to.
 */
class Replica(membership: Agent[Set[ActorRef]], performFun: Replica.PerformFun) extends Actor {

  val log = Logging(context.system, this)

  val leaders = membership
  var lowestUnusedSlotNum : Long = 1
  
  def propose(command: Command) {
    log.debug("Received proposal: assigning {} to {}", lowestUnusedSlotNum, command)
    leaders().foreach(_ ! Propose(lowestUnusedSlotNum, command))
    lowestUnusedSlotNum = lowestUnusedSlotNum + 1
  }
  
  def receive = {
    case Request(command: Command) => propose(command)
    case Decision(slot, command) =>
      log.debug("Received decision slot {} for {}", slot, command)
      if (slot >= lowestUnusedSlotNum) lowestUnusedSlotNum = slot + 1
      performFun(slot, command.op)
      command.client ! RequestPerformed
  }
}