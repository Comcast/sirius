package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.Actor
import akka.actor.ActorRef
import akka.util.duration._
import akka.event.Logging
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import java.util.{TreeMap => JTreeMap}
import scala.util.control.Breaks._

object Replica {

  case object Reap

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
   * @param reapWindow number of milliseconds for a proposal to live with the possibility
   *                         of being reproposed
   * @param reapScheduleFreq how often, in seconds, we reap old proposals
   */
  def apply(localLeader: ActorRef,
            startingSeqNum: Long,
            performFun: PerformFun,
            reapWindow: Long,
            reapScheduleFreq: Int): Replica = {

    new Replica(localLeader, startingSeqNum, performFun, reapWindow) {
      val reapCancellable =
        context.system.scheduler.schedule(reapScheduleFreq seconds,
                                          reapScheduleFreq seconds, self, Reap)
    }
  }
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
              performFun: Replica.PerformFun,
              reapWindow: Long) extends Actor {

  import Replica._

  val proposals = new JTreeMap[Long, Command]()

  val logger = Logging(context.system, "Sirius")
  val traceLogger = Logging(context.system, "SiriusTrace")

  var lowestUnusedSlotNum: Long = startingSeqNum

  /**
   * Propose a command to the local leader, either from a new Request or due
   * to a triggered reproposal.
   *
   * Has side-effect of incrementing lowestUnusedSlotNum and adding proposal to
   * proposals map.
   * @param command Command to be proposed
   */
  def propose(command: Command) {
    localLeader ! Propose(lowestUnusedSlotNum, command)
    proposals.put(lowestUnusedSlotNum, command)
    traceLogger.debug("Proposing slot {} for {}", lowestUnusedSlotNum, command)
    lowestUnusedSlotNum = lowestUnusedSlotNum + 1
  }

  def receive = {
    case GetLowestUnusedSlotNum => sender ! LowestUnusedSlotNum(lowestUnusedSlotNum)
    case Request(command: Command) =>
      propose(command)

    case decision @ Decision(slot, decidedCommand) =>
      traceLogger.debug("Received decision slot {} for {}",
        slot, decidedCommand)
      if (slot >= lowestUnusedSlotNum) {
        lowestUnusedSlotNum = slot + 1
      }

      checkForReproposal(slot, decidedCommand)

      try {
        performFun(decision)
      } catch {
        // XXX: is this too liberal?
        case t: Throwable =>
          logger.error("Received exception applying decision {}: {}", decision, t)
      }

    case Reap =>
      reapProposals()

  }

  /**
   * Check for items in proposals older than reapWindow seconds, delete them.
   * Uses timestamp in the Command, since that was generated in the local PaxosSup
   * and thus we don't have to worry about clock skew.
   */
  private def reapProposals() {
    val now = System.currentTimeMillis()
    breakable {
      val keys = proposals.keySet.toArray
      for (i <- 0 to keys.size - 1) {
        val slot = keys(i)
        if (proposals.get(slot).ts < now - reapWindow) {
          proposals.remove(slot)
        } else {
          break()
        }
      }
    }
  }

  /**
   * Check whether the decided command is one of the following:
   * - our proposal, in which case we can remove it from proposal list (succeeded)
   * - someone else's proposal, in which case we need to repropose our old proposal
   * - for a slot we haven't seen, in which case we do nothing
   *
   * @param slot sequence number for this command
   * @param decidedCommand command that has been decided for the sequence number
   * @return
   */
  def checkForReproposal(slot: Long, decidedCommand: Command) = proposals.get(slot) match {
    // decidedCommand is our proposed command, we can just remove it
    case proposedCommand: Command if decidedCommand == proposedCommand =>
      proposals.remove(slot)
    // decidedCommand is a different command; repropose our existing one
    case proposedCommand: Command =>
      traceLogger.debug("Recieved different decision for slot number " +
        "proposed by this Replica, reproposing {}", decidedCommand)
      propose(proposedCommand)
      proposals.remove(slot)
    case null =>
  }
}
