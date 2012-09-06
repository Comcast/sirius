package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.Actor
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.util.duration._
import akka.event.Logging
import java.util.{TreeMap => JTreeMap}
import scala.util.control.Breaks._
import scala.collection.JavaConverters._

object Acceptor {
  val reapWindow = 30 * 60 * 1000L

  case object Reap

  def apply(startingSeqNum: Long): Acceptor = {
    new Acceptor(startingSeqNum) {
      val reapCancellable =
        context.system.scheduler.schedule(30 seconds, 30 seconds, self, Reap)

      override def postStop() {
        reapCancellable.cancel()
      }
    }
  }
}

class Acceptor(startingSeqNum: Long) extends Actor {

  import Acceptor._

  val logger = Logging(context.system, "Sirius")
  val traceLogger = Logging(context.system, "SiriusTrace")

  var ballotNum: Ballot = Ballot.empty
  var accepted = new JTreeMap[Long, PValue]()

  // if we receive a Phase2A for a slot less than this we refuse to
  // handle it since it is out of date by our terms
  var lowestAcceptableSlotNumber: Long = startingSeqNum

  // Note that Phase1A and Phase2B requests must return their
  // parent's address (their PaxosSup) because this is how
  // the acceptor is known to external nodes
  def receive = {
    // Scout
    case Phase1A(scout, ballot, replyAs, latestDecidedSlot) =>
      if (ballot > ballotNum) ballotNum = ballot

      scout ! Phase1B(replyAs, ballotNum, undecidedAccepted(latestDecidedSlot).values.asScala.toSet)

    // Commander
    case Phase2A(commander, pval, replyAs) if pval.slotNum >= lowestAcceptableSlotNumber =>
      if (pval.ballot >= ballotNum) {
        ballotNum = pval.ballot

        //if pval is already accepted on higher ballot number then do nothing
        //    in other words
        // if pval not accepted or accepted and with lower or equal ballot number then replace accepted pval w/ pval
        accepted.get(pval.slotNum) match {
          case (oldPval: PValue) if oldPval.ballot > pval.ballot => accepted.put(oldPval.slotNum, oldPval)
          case (oldPval: PValue) => accepted.put(pval.slotNum, pval)
          case null => accepted.put(pval.slotNum, pval)
        }
      }
      commander ! Phase2B(replyAs, ballotNum)

    // Periodic cleanup
    case Reap =>
      logger.debug("Accepted count: {}", accepted.size)
      val (newLowestSlotNumber, newAccepted) = cleanOldAccepted(lowestAcceptableSlotNumber, accepted)
      lowestAcceptableSlotNumber = newLowestSlotNumber
      accepted = newAccepted
  }

  /* Remove 'old' pvals from the system.  A pval is old if we got it more than half an hour ago.
   * The timestamp on the pval came from the originating node, so we're using this wide window 
   * as a fudge factor for clock drift.
   * Note: this method has the side-effect of modifying toReap.
   */
  private def cleanOldAccepted(currentLowestSlot: Long, toReap: JTreeMap[Long, PValue]) = {
    var highestReapedSlot: Long = currentLowestSlot - 1
    val now = System.currentTimeMillis
    breakable {
      val keys = toReap.keySet.toArray
      for (i <- 0 to keys.size - 1) {
        val slot = keys(i)
        if (toReap.get(slot).proposedCommand.ts < now - reapWindow) {
          highestReapedSlot = toReap.get(slot).slotNum
          toReap.remove(slot)
        } else {
          break()
        }
      }
    }
    logger.debug("Reaped PValues for all commands between {} and {}", currentLowestSlot - 1, highestReapedSlot)
    (highestReapedSlot + 1, toReap)
  }

  /**
   * produces an undecided accepted tree
   *
   */
  private def undecidedAccepted(latestDecidedSlot: Long): JTreeMap[Long, PValue] ={
        val undecidedAcceptedTree = new JTreeMap[Long, PValue]()
        val iterator = accepted.keySet().iterator
        while (iterator.hasNext) {
          val slot = iterator.next
          if (slot > latestDecidedSlot) undecidedAcceptedTree.put(slot, accepted.get(slot))
        }
        undecidedAcceptedTree
  }

}