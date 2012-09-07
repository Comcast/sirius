package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.event.Logging

object Replica {

  /**
   * Clients must implement a function of this type and pass it in on
   * construction.  The function takes a Decision and should perform
   * any operation necessary to handle the decision.  Decisions may
   * arrive out of order and multiple times.  It is the responsibility
   * of the implementer to handle these cases.  Additionally, it is the
   * responsibility of the implementer to reply to client identified
   * by Decision.command.client.
   */
  type PerformFun = Decision => Unit

  /**
   * Create a Replica instance.
   *
   * The performFun argument must apply the operation, and return true indicating
   * that the operation was successfully performed/acknowledged, or return false
   * indicating that the operation was ignored.  When true is returned the initiating
   * actor of this request is sent the RequestPerformed message.  It is expected that
   * there is one actor per request.  When false is returned no such message is sent.
   * The reason for this is that multiple decisions may arrive for an individual slot.
   * While not absolutely necessary, this helps reduce chatter.
   *
   * Note this should be called from within a Props factory on Actor creation
   * due to the requirements of Akka.
   *
   * @param localLeader reference of replica's local {@link Leader}
   * @param performFun function specified by
   *          [[com.comcast.xfinity.sirius.api.impl.paxos.Replica.PerformFun]], applied to
   *          decisions as they arrive
   */
  def apply(localLeader: ActorRef,
            startingSeqNum: Long,
            performFun: PerformFun): Replica =
    new Replica(localLeader, startingSeqNum, performFun)
}

/**
 * TODO:
 * Must persist slotNum, lowestUnusedSlot to disk and read them in on initialization.
 * Must persist proposals and decisions to disk.
 * initialState is actually just an ActorRef to the State actor.
 * Keep track of old proposals and re-propose them, if we need to.
 */
class Replica(localLeader: ActorRef,
              startingSeqNum: Long,
              performFun: Replica.PerformFun) extends Actor {

  val logger = Logging(context.system, "Sirius")
  val traceLogger = Logging(context.system, "SiriusTrace")

  var lowestUnusedSlotNum: Long = startingSeqNum
  
  def propose(command: Command) {
    traceLogger.debug("Received proposal: assigning {} to {}", lowestUnusedSlotNum, command)
    localLeader ! Propose(lowestUnusedSlotNum, command)
    lowestUnusedSlotNum = lowestUnusedSlotNum + 1
  }

  def receive = {
    case GetLowestUnusedSlotNum => sender ! LowestUnusedSlotNum(lowestUnusedSlotNum)
    case Request(command: Command) => propose(command)
    case decision @ Decision(slot, command) =>
      traceLogger.debug("Received decision slot {} for {}",
        slot, command)
      if (slot >= lowestUnusedSlotNum) {
        lowestUnusedSlotNum = slot + 1
      }
      try {
        performFun(decision)
      } catch {
        // XXX: is this too liberal?
        case t: Throwable =>
          logger.error("Received exception applying decision {}: {}", decision, t)
      }
  }
}
