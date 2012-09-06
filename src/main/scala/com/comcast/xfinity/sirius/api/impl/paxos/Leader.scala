package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{ Props, Actor, ActorRef }
import akka.agent.Agent
import collection.immutable.SortedMap
import annotation.tailrec
import akka.util.duration._
import akka.event.Logging
import java.util.UUID
import java.util.{TreeMap => JTreeMap}
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

object Leader {
  object Reap

  trait HelperProvider {
    val leaderHelper: LeaderHelper
    def startCommander(pval: PValue): Unit
    def startScout(): Unit
  }

  def apply(membership: Agent[Set[ActorRef]],
            startingSeqNum: Long): Leader = {
    new Leader(membership, startingSeqNum) with HelperProvider {
      val reapCancellable =
        context.system.scheduler.schedule(30 seconds, 30 seconds, self, Reap)

      override def postStop() { reapCancellable.cancel() }

      val leaderHelper = new LeaderHelper()

      def startCommander(pval: PValue) {
        // XXX: more members may show up between when acceptors() and replicas(),
        //      we may want to combine the two, and just reference membership
        context.actorOf(Props(new Commander(self, acceptors(), replicas(), pval)))
      }

      def startScout() {
        context.actorOf(Props(new Scout(self, acceptors(), ballotNum)))
      }
    }
  }
}

class Leader(membership: Agent[Set[ActorRef]],
             startingSeqNum: Long) extends Actor {
    this: Leader.HelperProvider =>

  import Leader.Reap

  val log = Logging(context.system, this)

  val acceptors = membership
  val replicas = membership

  var ballotNum = Ballot(0, UUID.randomUUID().toString)
  var active = false
  var proposals = new JTreeMap[Long, Command]()

  var lowestAcceptableSlot: Long = startingSeqNum

  log.info("Starting leader using ballotNum={}", ballotNum)

  startScout()

  def receive = {
    case Propose(slotNum, command) if !proposals.containsKey(slotNum) =>
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

    case Reap =>
      //XXX:  This should be removed and replaced with proper monitoring soon.
      log.info("Proposal count:  " +  proposals.size)
      val (newLowestSlot, newProposals) = filterOldProposals(lowestAcceptableSlot, proposals)
      proposals = newProposals
      lowestAcceptableSlot = newLowestSlot


    // if our scout fails to make progress, retry
    case ScoutTimeout => startScout()
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
  private def filterOldProposals(currentLowestSlot: Long, toClean: JTreeMap[Long, Command]) = {
    val thirtyMinutesAgo = System.currentTimeMillis() - (30 * 60 * 1000L)
    var highestReapedSlot = currentLowestSlot - 1

    breakable {
      for (key <- toClean.keySet.toArray) {
        val slot = key.asInstanceOf[Long]
        if (toClean.get(slot).ts < thirtyMinutesAgo) {
          highestReapedSlot = slot
          toClean.remove(slot)
        } else {
          // break on first that does not need to be reaped
          break
        }
      }
    }

    log.debug("Reaped proposals for slots {} through {}", currentLowestSlot - 1, highestReapedSlot)

    (highestReapedSlot + 1, toClean)
  }
}