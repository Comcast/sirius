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
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderPinger.{Pong, Ping}
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.{SeekLeadership, Close}
import com.comcast.xfinity.sirius.util.{RichJTreeMap, AkkaExternalAddressResolver}

object Leader {
  trait HelperProvider {
    val leaderHelper: LeaderHelper
    def startCommander(pval: PValue, ticks: Int = 0): Unit
    def startScout(): Unit
  }

  def apply(membership: Agent[Set[ActorRef]],
            startingSeqNum: Long,
            config: SiriusConfiguration): Leader = {

    //XXX make configurable!
    val defaultRetries = 2

    new Leader(membership, startingSeqNum)(config) with HelperProvider {
      val leaderHelper = new LeaderHelper()

      def startCommander(pval: PValue, ticks: Int = defaultRetries) {
        // XXX: more members may show up between when acceptors() and replicas(),
        //      we may want to combine the two, and just reference membership
        context.actorOf(Props(new Commander(self, acceptors(), replicas(), pval, ticks)))
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
  val traceLogger = Logging(context.system, "SiriusTrace")

  val acceptors = membership
  val replicas = membership

  val myLeaderId = AkkaExternalAddressResolver(context.system).externalAddressFor(self)
  var ballotNum = Ballot(0, myLeaderId)
  var proposals = new RichJTreeMap[Long, Command]()

  var latestDecidedSlot: Long = startingSeqNum - 1

  var electedLeaderBallot: Option[Ballot] = None
  var currentLeaderWatcher: Option[ActorRef] = None

  // XXX for monitoring...
  var longestReapDuration = 0L
  var lastReapDuration = 0L
  var currentLeaderElectedSince = 0L
  var commanderTimeoutCount = 0L
  var lastTimedOutPValue: Option[PValue] = None

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
        case Some(electedBallot) if (ballotNum == electedBallot) =>
          proposals.put(slotNum, command)
          startCommander(PValue(ballotNum, slotNum, command))
        case Some(electedBallot @ Ballot(_, leaderId)) if (ballotNum != electedBallot) =>
          context.actorFor(leaderId) forward propose
        case None =>
          proposals.put(slotNum, command)
      }

    case Adopted(newBallotNum, pvals) if ballotNum == newBallotNum =>
      logger.debug("Assuming leadership using {}", ballotNum)

      // XXX: update actually has side effects, however this assignment
      //      is necessary for testing :/ removing in a later commit
      proposals = leaderHelper.update(proposals, leaderHelper.pmax(pvals))
      proposals.foreach(
        (slot, command) => startCommander(PValue(ballotNum, slot, command))
      )
      currentLeaderElectedSince = System.currentTimeMillis()
      electedLeaderBallot = Some(ballotNum)

    // there's a new leader, update electedLeaderBallot and start a new watcher accordingly
    case Preempted(newBallot) if newBallot > ballotNum =>
      logger.debug("Becoming subservient to new leader with ballot {}", newBallot)
      currentLeaderElectedSince = System.currentTimeMillis()
      electedLeaderBallot = Some(newBallot)
      val electedLeader = context.actorFor(newBallot.leaderId)
      proposals.foreach(
        (slot, command) => electedLeader ! Propose(slot, command)
      )
      stopLeaderWatcher(currentLeaderWatcher)
      currentLeaderWatcher = Some(createLeaderWatcher(newBallot, self))

    // try to become the new leader; old leader has gone MIA
    case SeekLeadership =>
      /* Get the sequence number that should be used in a new
       * ballot.  If there is (or was) an elected leader, increment that
       * ballot number; otherwise increment our own.
       */
      ballotNum = Ballot(electedLeaderBallot match {
        case Some(Ballot(seq, _)) => seq + 1
        case _ => ballotNum.seq + 1
      }, myLeaderId)

      electedLeaderBallot = None

      stopLeaderWatcher(currentLeaderWatcher)
      currentLeaderWatcher = None

      startScout()

    // respond to Ping from LeaderPinger with our current leader ballot information
    case Ping =>
      sender ! Pong(electedLeaderBallot)

    // if our scout fails to make progress, retry
    case ScoutTimeout =>
      if (electedLeaderBallot == None) startScout()

    // if the commander times out we nullify it's slot in our proposals
    //  and let someone else try out
    case Commander.CommanderTimeout(pvalue, ticks) =>
      traceLogger.debug("Commander timed out for {}", pvalue)

      if (ticks > 0) {
        traceLogger.debug("Restarting commander for {}, {} ticks left", pvalue, ticks - 1)
        startCommander(pvalue, ticks - 1)
      } else {
        proposals.remove(pvalue.slotNum)
      }

      // some record keeping
      commanderTimeoutCount += 1
      lastTimedOutPValue = Some(pvalue)

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

  // drops all proposals held locally whos slot is <= latestDecidedSlot
  private def reapProposals() {
    val start = System.currentTimeMillis
    proposals.dropWhile(
      (slot, _) => slot <= latestDecidedSlot
    )
    val duration = System.currentTimeMillis() - start

    logger.debug("Reaped old proposals up to {} in {}ms", latestDecidedSlot, duration)

    lastReapDuration = duration
    if (duration > longestReapDuration)
      longestReapDuration = duration
  }

  // monitoring hooks, to close over the scope of the class, it has to be this way
  //  because of jmx
  trait LeaderInfoMBean {
    def getBallotNum: String
    def getLatestDecidedSlot: Long
    def getProposalCount: Int
    def getElectedLeaderBallot: String
    def getCurrentLeaderElectedSince: Long
    def getLongestReapDuration: Long
    def getLastReapDuration: Long
    def getCommanderTimeoutCount: Long
    def getLastTimedOutPValue: String
  }

  class LeaderInfo extends LeaderInfoMBean{
    def getBallotNum = ballotNum.toString
    def getLatestDecidedSlot = latestDecidedSlot
    def getProposalCount = proposals.size
    def getElectedLeaderBallot = electedLeaderBallot.toString
    def getCurrentLeaderElectedSince = currentLeaderElectedSince
    def getLongestReapDuration = longestReapDuration
    def getLastReapDuration = lastReapDuration
    def getCommanderTimeoutCount = commanderTimeoutCount
    def getLastTimedOutPValue = lastTimedOutPValue.toString
  }
}
