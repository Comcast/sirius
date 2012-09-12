package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{Props, Actor, ActorRef}
import akka.agent.Agent
import akka.event.Logging
import java.util.{TreeMap => JTreeMap}
import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderPinger.{Pong, Ping}
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.{SeekLeadership, Close}

object Leader {
  trait HelperProvider {
    val leaderHelper: LeaderHelper
    def startCommander(pval: PValue): Unit
    def startScout(): Unit
  }

  def apply(membership: Agent[Set[ActorRef]],
            startingSeqNum: Long,
            config: SiriusConfiguration): Leader = {
    new Leader(membership, startingSeqNum)(config) with HelperProvider {
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
             startingSeqNum: Long)
            (implicit config: SiriusConfiguration = new SiriusConfiguration)
      extends Actor with MonitoringHooks {
    this: Leader.HelperProvider =>

  val logger = Logging(context.system, "Sirius")

  val acceptors = membership
  val replicas = membership

  val myLeaderId = AkkaExternalAddressResolver(context.system).externalAddressFor(self)
  var ballotNum = Ballot(0, myLeaderId)
  var active = false
  var proposals = new JTreeMap[Long, Command]()

  var latestDecidedSlot: Long = startingSeqNum - 1

  var electedLeaderBallot: Option[Ballot] = None
  var currentLeaderWatcher: Option[ActorRef] = None

  logger.info("Starting leader using ballotNum={}", ballotNum)

  startScout()

  override def preStart() {
    registerMonitor(new LeaderInfo, config)
  }

  override def postStop() {
    unregisterMonitors(config)
  }

  def receive = {
    case propose @ Propose(slotNum, command) if !proposals.containsKey(slotNum) && slotNum > latestDecidedSlot =>
      electedLeaderBallot match {
        case Some(electedBallot) if (ballotNum == electedBallot && active == true) =>
          proposals.put(slotNum, command)
          startCommander(PValue(ballotNum, slotNum, command))
        case Some(electedBallot @ Ballot(_, leaderId)) if (ballotNum != electedBallot) =>
          context.actorFor(leaderId) forward propose
        case None =>
          proposals.put(slotNum, command)
      }

    case Adopted(newBallotNum, pvals) if ballotNum == newBallotNum =>
      proposals = leaderHelper.update(proposals, leaderHelper.pmax(pvals))
      for (slot <- proposals.keySet) {
        startCommander(PValue(ballotNum, slot, proposals.get(slot)))
      }
      active = true
      electedLeaderBallot = Some(ballotNum)

    // there's a new leader, update electedLeaderBallot and start a new watcher accordingly
    case Preempted(newBallot) if newBallot > ballotNum =>
      active = false
      electedLeaderBallot = Some(newBallot)
      val electedLeader = context.actorFor(newBallot.leaderId)
      for (slot <- proposals.keySet) {
        electedLeader ! Propose(slot, proposals(slot))
      }
      stopLeaderWatcher(currentLeaderWatcher)
      currentLeaderWatcher = Some(createLeaderWatcher(newBallot, self))

    // try to become the new leader; old leader has gone MIA
    case SeekLeadership =>
      active = false
      electedLeaderBallot = None

      stopLeaderWatcher(currentLeaderWatcher)
      currentLeaderWatcher = None

      ballotNum = Ballot(ballotNum.seq + 1, ballotNum.leaderId)
      startScout()

    // respond to Ping from LeaderPinger with our current leader ballot information
    case Ping =>
      sender ! Pong(electedLeaderBallot)

    // if our scout fails to make progress, retry
    case ScoutTimeout => startScout()

    // the SirusPaxosBridge will notify the Leader of the last decision.  We can then use this to reduce the number
    // of accepted decisions we need from the Acceptor
    case DecisionHint(lastSlot) =>
      latestDecidedSlot = lastSlot
      reapProposals()

  }

  /**
   * kill this leaderWatcher, if it is instantiated and is alive
   */
  private[paxos] def stopLeaderWatcher(leaderWatcher: Option[ActorRef]) {
    leaderWatcher match {
      case Some(ref) if (!ref.isTerminated) => ref ! Close
      case _ =>
    }
  }

  private[paxos] def createLeaderWatcher(newBallot: Ballot, replyTo: ActorRef) = {
    context.actorOf(Props(new LeaderWatcher(newBallot, replyTo)))
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

  // monitoring hooks, to close over the scope of the class, it has to be this way
  //  because of jmx
  trait LeaderInfoMBean {
    def getBallotNum: String
    def getActive: Boolean
    def getLatestDecidedSlot: Long
    def getProposalCount: Int
  }

  class LeaderInfo extends LeaderInfoMBean{
    def getBallotNum = ballotNum.toString
    def getActive = active
    def getLatestDecidedSlot = latestDecidedSlot
    def getProposalCount = proposals.size
  }
}
