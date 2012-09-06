package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{ Props, Actor, ActorRef }
import akka.agent.Agent
import akka.util.duration._
import akka.event.Logging
import java.util.UUID
import java.util.{TreeMap => JTreeMap}
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

object Leader {
  trait HelperProvider {
    val leaderHelper: LeaderHelper
    def startCommander(pval: PValue): Unit
    def startScout(): Unit
  }

  def apply(membership: Agent[Set[ActorRef]],
            startingSeqNum: Long): Leader = {
    new Leader(membership, startingSeqNum) with HelperProvider {
      val leaderHelper = new LeaderHelper()

      def startCommander(pval: PValue) {
        // XXX: more members may show up between when acceptors() and replicas(),
        //      we may want to combine the two, and just reference membership
        context.actorOf(Props(new Commander(self, acceptors(), replicas(), pval)))
      }

      def startScout() {
        context.actorOf(Props(new Scout(self, acceptors(), ballotNum, latestDecidedSlot)))
      }
    }
  }
}

class Leader(membership: Agent[Set[ActorRef]],
             startingSeqNum: Long) extends Actor {
    this: Leader.HelperProvider =>

  val logger = Logging(context.system, "Sirius")

  val acceptors = membership
  val replicas = membership

  var ballotNum = Ballot(0, UUID.randomUUID().toString)
  var active = false
  var proposals = new JTreeMap[Long, Command]()

  var latestDecidedSlot: Long = startingSeqNum - 1

  logger.info("Starting leader using ballotNum={}", ballotNum)

  startScout()

  def receive = {
    case Propose(slotNum, command) if !proposals.containsKey(slotNum) && slotNum > latestDecidedSlot =>
      proposals.put(slotNum, command)
      if (active) {
        startCommander(PValue(ballotNum, slotNum, command))
      }

    case Adopted(newBallotNum, pvals) if ballotNum == newBallotNum =>
      proposals = leaderHelper.update(proposals, leaderHelper.pmax(pvals))
      for (slot <- proposals.keySet) {
        startCommander(PValue(ballotNum, slot, proposals.get(slot)))
      }
      active = true

    case Preempted(newBallot) if newBallot > ballotNum =>
      active = false
      ballotNum = Ballot(newBallot.seq + 1, ballotNum.leaderId)
      startScout()

    // if our scout fails to make progress, retry
    case ScoutTimeout => startScout()

    // the SirusPaxosBridge will notify the Leader of the last decision.  We can then use this to reduce the number
    // of accepted decisions we need from the Acceptor
    case DecisionHint(lastSlot) =>
      latestDecidedSlot = lastSlot
      reapProposals()
  }

  private def reapProposals() {
    val newProposals = filterOldProposals(proposals)
    proposals = newProposals
  }

  /**
   * Drops all all items from the beginning of toClean who's timestamp is greater than thirty
   * minutes ago, until it encounters an item whose timestamp is current.  For example if the items
   * in positions 1 and 3 were outdated, but the item in position 2 was current, only 1 would
   * be dropped.  Returns the tuple of the position of the next highest command it did not reap,
   * and the rest of the map after cleaning.
   *
   * This also has the side effect of altering toClean.  Send in a clone (don't you love farce?)
   * if you need to keep the original.
   */
  private def filterOldProposals(toClean: JTreeMap[Long, Command]) = {
    breakable {
      for (key <- toClean.keySet.toArray) {
        val slot = key.asInstanceOf[Long]
        if (slot <= latestDecidedSlot) {
          toClean.remove(slot)
        } else {
          // break on first that does not need to be reaped
          break()
        }
      }
    }

    logger.debug("Reaped proposals for slots up to {}", latestDecidedSlot)

    toClean
  }
}