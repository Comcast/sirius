package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{ Props, Actor, ActorRef }
import akka.agent.Agent
import collection.immutable.SortedMap
import annotation.tailrec
import akka.util.duration._
import akka.event.Logging
import java.util.UUID

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
  var proposals = SortedMap[Long, Command]()
  var lowestAcceptableSlot: Long = startingSeqNum

  log.info("Starting leader using ballotNum={}", ballotNum)

  startScout()

  def receive = {
    case Propose(slotNum, command) if !proposals.contains(slotNum) =>
      proposals += (slotNum -> command)
      if (active) {
        startCommander(PValue(ballotNum, slotNum, command))
      }

    case Adopted(newBallotNum, pvals) if ballotNum == newBallotNum =>
      proposals = leaderHelper.update(proposals, leaderHelper.pmax(pvals))
      proposals.foreach {
        case (slot, command) => startCommander(PValue(ballotNum, slot, command))
      }
      active = true

    case Preempted(newBallot) if newBallot > ballotNum =>
      active = false
      ballotNum = Ballot(newBallot.seq + 1, ballotNum.leaderId)
      startScout()

    case Reap =>
      val (newLowestSlot, newProposals) = filterOldProposals(lowestAcceptableSlot, proposals)
      proposals = newProposals
      lowestAcceptableSlot = newLowestSlot


    // if our scout fails to make progress, retry
    case ScoutTimeout => startScout()
  }

  /**
   * Drops all all items from the beginning of toClean who's timestamp is greater than thirty
   * minutes ago, until it encounters an item whos timestamp is current.  For example if the items
   * in positions 1 and 3 were outdated, but the item in position 2 was current, only 1 would
   * be dropped.  Returns the tuple of the position of the next highest command it did not reap,
   * and the rest of the map after cleaning.
   */
  private def filterOldProposals(currentLowestSlot: Long, toClean: SortedMap[Long, Command]) = {
    val thirtyMinutesAgo = System.currentTimeMillis() - (30 * 60 * 1000L)
    var highestReapedSlot = currentLowestSlot - 1

    /**
     * Due to some weirdness around dropWhile, we needed to make our own
     */
    @tailrec
    def trueDropWhile[A, B](sortedMap: SortedMap[A, B])(pred: (A, B) => Boolean): SortedMap[A, B] =
      sortedMap.headOption match {
        case Some((k, v)) if pred(k, v) => trueDropWhile(sortedMap.tail)(pred)
        case _ => sortedMap
      }

    // XXX: has the side effect of picking the highest reape
    val cleaned = trueDropWhile(toClean) {
      case (slot, Command(_, timestamp, _)) if timestamp < thirtyMinutesAgo =>
        highestReapedSlot = slot
        true
      case (slot, _) =>
        false
    }

    log.debug("Reaped proposals for slots {} through {}", currentLowestSlot - 1, highestReapedSlot)

    (highestReapedSlot + 1, cleaned)
  }
}