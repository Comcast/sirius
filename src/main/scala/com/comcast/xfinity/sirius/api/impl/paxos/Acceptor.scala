package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.Actor
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.util.duration._
import akka.event.Logging
import java.util.{TreeMap => JTreeMap}
import scala.util.control.Breaks._
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import collection.immutable.HashSet
import collection.mutable.SetBuilder
import com.comcast.xfinity.sirius.admin.MonitoringHooks

object Acceptor {

  case object Reap

  def apply(startingSeqNum: Long, config: SiriusConfiguration): Acceptor = {
    val reapWindow = config.getProp(SiriusConfiguration.ACCEPTOR_WINDOW, 10 * 60 * 1000L)
    val reapFreqSecs = config.getProp(SiriusConfiguration.ACCEPTOR_CLEANUP_FREQ, 30)

    new Acceptor(startingSeqNum, reapWindow)(config) {
      val reapCancellable =
        context.system.scheduler.schedule(reapFreqSecs seconds, reapFreqSecs seconds, self, Reap)

      override def preStart() {
        registerMonitor(new AcceptorInfo, config)
      }
      override def postStop() {
        unregisterMonitors(config)
        reapCancellable.cancel()
      }
    }
  }
}

class Acceptor(startingSeqNum: Long,
               reapWindow: Long = 10 * 60 * 1000L)
              (implicit config: SiriusConfiguration = new SiriusConfiguration) extends Actor with MonitoringHooks {

  import Acceptor._

  val logger = Logging(context.system, "Sirius")
  val traceLogger = Logging(context.system, "SiriusTrace")

  var ballotNum: Ballot = Ballot.empty


  // XXX for monitoring...
  var lastDuration = 0L
  var longestDuration = 0L

  // slot -> (ts,PValue)
  var accepted = new JTreeMap[Long, Tuple2[Long, PValue]]()

  // if we receive a Phase2A for a slot less than this we refuse to
  // handle it since it is out of date by our terms
  var lowestAcceptableSlotNumber: Long = startingSeqNum

  // Note that Phase1A and Phase2B requests must return their
  // parent's address (their PaxosSup) because this is how
  // the acceptor is known to external nodes
  def receive = {
    // Scout
    case Phase1A(scout, ballot, replyAs, latestDecidedSlot) =>
      if (ballot > ballotNum) {
        ballotNum = ballot
      }

      scout ! Phase1B(replyAs, ballotNum, undecidedAccepted(latestDecidedSlot))

    // Commander
    case Phase2A(commander, pval, replyAs) if pval.slotNum >= lowestAcceptableSlotNumber =>
      if (pval.ballot >= ballotNum) {
        ballotNum = pval.ballot

        //if pval is already accepted on higher ballot number then do nothing
        //    in other words
        // if pval not accepted or accepted and with lower or equal ballot number then replace accepted pval w/ pval
        // also update the ts w/ localtime in our accepted map for reaping
        val now = System.currentTimeMillis
        accepted.get(pval.slotNum) match {
          case ((_, oldPval: PValue)) if oldPval.ballot > pval.ballot => accepted.put(oldPval.slotNum, (now, oldPval))
          case ((_, oldPval: PValue)) => accepted.put(pval.slotNum, (now, pval))
          case null => accepted.put(pval.slotNum, (now, pval))
        }
      }
      commander ! Phase2B(replyAs, ballotNum)

    // Periodic cleanup
    case Reap =>
      logger.debug("Accepted count: {}", accepted.size)
      val start = System.currentTimeMillis
      val (newLowestSlotNumber, newAccepted) = cleanOldAccepted(lowestAcceptableSlotNumber, accepted)
      lowestAcceptableSlotNumber = newLowestSlotNumber
      logger.debug("Reaped Old Accpeted in {}ms", System.currentTimeMillis-start)
      accepted = newAccepted
  }

  /* Remove 'old' pvals from the system.  A pval is old if we got it farther in the past than our reap limit.
   * The timestamp in the tuple with the pval came from localtime when we received it in the Phase2A msg.
   *
   * Note: this method has the side-effect of modifying toReap.
   */
  private def cleanOldAccepted(currentLowestSlot: Long, toReap: JTreeMap[Long, Tuple2[Long, PValue]]) = {
    var highestReapedSlot: Long = currentLowestSlot - 1
    val now = System.currentTimeMillis()
    val reapBeforeTs = now - reapWindow
    breakable {
      val keys = toReap.keySet.toArray
      for (i <- 0 to keys.size - 1) {
        val slot = keys(i)
        if (toReap.get(slot)._1 < reapBeforeTs) {
          highestReapedSlot = toReap.get(slot)._2.slotNum
          toReap.remove(slot)
        } else {
          break()
        }
      }
    }
    val duration = System.currentTimeMillis() - now
    lastDuration = duration
    if (duration > longestDuration)
      longestDuration = duration

    logger.debug("Reaped PValues for all commands between {} and {}", currentLowestSlot - 1, highestReapedSlot)
    (highestReapedSlot + 1, toReap)
  }

  /**
   * produces an undecided accepted Set of PValues
   *
   */
  private def undecidedAccepted(latestDecidedSlot: Long): Set[PValue] = {
    val empty = new HashSet[PValue]()
    var undecidedPvalues = new SetBuilder[PValue, HashSet[PValue]](empty)
    val iterator = accepted.keySet().iterator
    while (iterator.hasNext) {
      val slot = iterator.next
      if (slot > latestDecidedSlot) {
        undecidedPvalues += accepted.get(slot)._2
      }
    }
    undecidedPvalues.result()
  }

  /**
   * Monitoring hooks
   */
  trait AcceptorInfoMBean {
    def getAcceptedSize: Int
    def getLowestAcceptableSlotNum: Long
    def getBallot: Ballot
    def getLastDuration: Long
    def getLongestDuration: Long
  }

  class AcceptorInfo extends AcceptorInfoMBean {
    def getAcceptedSize = accepted.size
    def getLowestAcceptableSlotNum = lowestAcceptableSlotNumber
    def getBallot = ballotNum
    def getLastDuration = lastDuration
    def getLongestDuration = longestDuration
  }
}