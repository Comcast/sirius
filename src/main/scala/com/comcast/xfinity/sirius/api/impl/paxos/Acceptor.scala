package com.comcast.xfinity.sirius.api.impl.paxos
import akka.actor.Actor
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import scala.collection.immutable.SortedMap
import akka.util.duration._
import annotation.tailrec
import akka.event.Logging

object Acceptor {
  case object Reap

  def apply(startingSeqNum: Long): Acceptor = {
    new Acceptor(startingSeqNum) {
      val reapCancellable =
        context.system.scheduler.schedule(30 seconds, 30 seconds , self, Reap)

      override def postStop() { reapCancellable.cancel() }
    }
  }
}

class Acceptor(startingSeqNum: Long) extends Actor {
  import Acceptor._

  val log = Logging(context.system, this)

  var ballotNum: Ballot = Ballot.empty
  var accepted = SortedMap[Long, PValue]()

  // if we receive a Phase2A for a slot less than this we refuse to
  // handle it since it is out of date by our terms
  var lowestAcceptableSlotNumber: Long = startingSeqNum

  // Note that Phase1A and Phase2B requests must return their
  // parent's address (their PaxosSup) because this is how
  // the acceptor is known to external nodes
  def receive = {
    // Scout
    case Phase1A(scout, ballot, replyAs) =>
      if (ballot > ballotNum) ballotNum = ballot
      scout ! Phase1B(replyAs, ballotNum, accepted.values.toSet)

    // Commander
    case Phase2A(commander, pval, replyAs) if pval.slotNum >= lowestAcceptableSlotNumber =>
      if (pval.ballot >= ballotNum) {
        ballotNum = pval.ballot
        accepted += (accepted.get(pval.slotNum) match {
          case Some(oldPval) if oldPval.ballot > pval.ballot => (oldPval.slotNum -> oldPval)
          case _ => (pval.slotNum -> pval)
        })
      }
      commander ! Phase2B(replyAs, ballotNum)

    // Periodic cleanup
    case Reap =>
      val (newLowestSlotNumber, newAccepted) = cleanOldAccepted(lowestAcceptableSlotNumber, accepted)
      lowestAcceptableSlotNumber = newLowestSlotNumber
      accepted = newAccepted
  }

  /* Remove 'old' pvals from the system.  A pval is old if we got it more than half an hour ago.
   * The timestamp on the pval came from the originating node, so we're using this wide window 
   * as a fudge factor for clock drift.
   */
  private def cleanOldAccepted(currentLowestSlot : Long, toReap : SortedMap[Long, PValue]) = {
    val thirtyMinutesAgo = 30 * 60 * 1000L
    var highestReapedSlot: Long = currentLowestSlot - 1

    /**
     * Due to some weirdness around dropWhile, we needed to make our own
     */
    @tailrec
    def trueDropWhile[A, B](sortedMap: SortedMap[A, B])(pred: (A, B) => Boolean): SortedMap[A, B] =
      sortedMap.headOption match {
        case Some((k, v)) if pred(k, v) => trueDropWhile(sortedMap.tail)(pred)
        case _ => sortedMap
      }

    val newAccepted = trueDropWhile(toReap) {
      case(slot, PValue(_, _, Command(_, timestamp, _))) if timestamp < thirtyMinutesAgo => {
        highestReapedSlot = slot
        true
      }
      case _ => false
    }

    log.debug("Reaped PValues for all commands between {} and {}", currentLowestSlot - 1, highestReapedSlot)

    (highestReapedSlot + 1, newAccepted)
  }
}