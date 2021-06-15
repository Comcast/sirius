/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.{Props, Actor}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import scala.concurrent.duration._
import akka.event.Logging
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.util.RichJTreeMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

object Acceptor {

  case object Reap

  /**
   * Create Props for an Acceptor actor.
   *
   * @param startingSeqNum sequence number at which to start accepting
   * @param config siriusConfiguration object full of all kinds of configuration goodies, see SiriusConfiguration for
   *               more information
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(startingSeqNum: Long, config: SiriusConfiguration): Props = {
    val reapWindow = config.getProp(SiriusConfiguration.ACCEPTOR_WINDOW, 10 * 60 * 1000L)
    val reapFreqSecs = config.getProp(SiriusConfiguration.ACCEPTOR_CLEANUP_FREQ, 30)
    Props(classOf[Acceptor], startingSeqNum, reapWindow, reapFreqSecs, config)
  }
}

class Acceptor(startingSeqNum: Long,
               reapWindow: Long,
               reapFreqSecs: Int,
               config: SiriusConfiguration) extends Actor with MonitoringHooks {

  import Acceptor._

  val reapCancellable =
    context.system.scheduler.schedule(reapFreqSecs seconds, reapFreqSecs seconds, self, Reap)

  override def preStart(): Unit = {
    registerMonitor(new AcceptorInfo, config)
  }
  override def postStop(): Unit = {
    unregisterMonitors(config)
    reapCancellable.cancel()
  }

  val logger = Logging(context.system, "Sirius")
  val traceLogger = Logging(context.system, "SiriusTrace")

  var ballotNum: Ballot = Ballot.empty

  // XXX for monitoring...
  var lastDuration = 0L
  var longestDuration = 0L

  // slot -> (ts,PValue)
  var accepted = RichJTreeMap[Long, Tuple2[Long, PValue]]()

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

        // if pval is already accepted on higher ballot number then just update the timestamp
        //    in other words
        // if pval not accepted or accepted and with lower or equal ballot number then replace accepted pval w/ pval
        // also update the ts w/ localtime in our accepted map for reaping
        accepted.get(pval.slotNum) match {
          case (_, oldPval) if oldPval.ballot > pval.ballot =>
            accepted.put(oldPval.slotNum, (System.currentTimeMillis, oldPval))
          case _ =>
            accepted.put(pval.slotNum, (System.currentTimeMillis, pval))
        }
      }
      commander ! Phase2B(replyAs, ballotNum)

    // Periodic cleanup
    case Reap =>
      logger.debug("Accepted count: {}", accepted.size)

      val start = System.currentTimeMillis
      cleanOldAccepted()
      val duration = System.currentTimeMillis - start

      lastDuration = duration
      if (duration > longestDuration)
        longestDuration = duration

      logger.debug("Reaped Old Accepted in {}ms", System.currentTimeMillis-start)
  }

  /* Remove 'old' pvals from the system.  A pval is old if we got it farther in the past than our reap limit.
   * The timestamp in the tuple with the pval came from localtime when we received it in the Phase2A msg.
   *
   * Note: this method has the side-effect of modifying toReap.
   */
  private def cleanOldAccepted(): Unit = {
    var highestReapedSlot: Long = lowestAcceptableSlotNumber - 1
    val reapBeforeTs = System.currentTimeMillis - reapWindow
    accepted.dropWhile {
      case (slot, (ts, _)) if ts < reapBeforeTs =>
        highestReapedSlot = slot
        true
      case _ => false
    }
    logger.debug("Reaped PValues for all commands between {} and {}", lowestAcceptableSlotNumber - 1, highestReapedSlot)
    lowestAcceptableSlotNumber = highestReapedSlot + 1
  }

  /**
   * produces an undecided accepted Set of PValues
   *
   */
  private def undecidedAccepted(latestDecidedSlot: Long): Set[PValue] = {
    var undecidedPValues = Set[PValue]()
    accepted.foreach {
      case (slot, (_, pval)) if slot > latestDecidedSlot =>
        undecidedPValues += pval
      case _ => //no-op
    }
    undecidedPValues
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
