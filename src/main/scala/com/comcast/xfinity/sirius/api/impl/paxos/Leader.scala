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

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor._
import akka.event.Logging
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderPinger.{Ping, Pong}
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.{LeaderGone, Close}
import com.comcast.xfinity.sirius.util.{RichJTreeMap, AkkaExternalAddressResolver}
import com.comcast.xfinity.sirius.api.impl.paxos.Leader.ChildProvider
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Preempted
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.PValue
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.DecisionHint
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Propose
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Adopted
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Command
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper.ClusterInfo
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.language.postfixOps

object Leader {

  trait ElectedLeader
  case object Unknown extends ElectedLeader
  case object Local extends ElectedLeader
  case class Remote(ref: ActorRef, ballot: Ballot) extends ElectedLeader

  /**
   * Factory for creating the children actors of Leader.
   *
   * @param config the SiriusConfiguration for this node
   */
  private[paxos] class ChildProvider(config: SiriusConfiguration){
    def createCommander(leader: ActorRef, clusterInfo: ClusterInfo, pval: PValue, ticks: Int)
                       (implicit context: ActorContext): ActorRef = {
      context.actorOf(Commander.props(leader, clusterInfo, pval, ticks))
    }

    def createScout(leader: ActorRef, clusterInfo: ClusterInfo, myBallot: Ballot, latestDecidedSlot: Long)
                   (implicit context: ActorContext): ActorRef = {
      context.actorOf(Scout.props(leader, clusterInfo, myBallot, latestDecidedSlot))
    }

    def createLeaderWatcher(leader: ActorRef, ballotToWatch: Ballot, replyTo: ActorRef)(implicit context: ActorContext): ActorRef = {
       context.actorOf(LeaderWatcher.props(leader, ballotToWatch, replyTo, config))
    }
  }
  /**
   * Create Props for Leader actor.
   *
   * @param membership membershipHelper for querying about cluster information
   * @param startingSeqNum the sequence number at which this node will begin issuing/acknowledging
   * @param config SiriusConfiguration for this node
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
   def props(membership: MembershipHelper,
             startingSeqNum: Long,
             config: SiriusConfiguration): Props = {
     val childProvider = new ChildProvider(config)
     val leaderHelper = new LeaderHelper

     Props(classOf[Leader], membership, startingSeqNum, childProvider, leaderHelper, config)
   }
}

class Leader(membership: MembershipHelper,
             startingSeqNum: Long,
             childProvider: ChildProvider,
             leaderHelper: LeaderHelper,
             config: SiriusConfiguration)
      extends Actor with MonitoringHooks {
    import Leader._

  implicit val executionContext = context.dispatcher

  val logger = Logging(context.system, "Sirius")
  val traceLogger = Logging(context.system, "SiriusTrace")
  val akkaExternalAddressResolver = config.getProp[AkkaExternalAddressResolver](SiriusConfiguration.AKKA_EXTERNAL_ADDRESS_RESOLVER).
    getOrElse(throw new IllegalStateException("SiriusConfiguration.AKKA_EXTERNAL_ADDRESS_RESOLVER returned nothing"))
  val myLeaderId = akkaExternalAddressResolver.externalAddressFor(self)
  var myBallot = Ballot(0, myLeaderId)
  var proposals = RichJTreeMap[Long, Command]()

  var latestDecidedSlot: Long = startingSeqNum - 1

  var electedLeader: ElectedLeader = Unknown
  var currentLeaderWatcher: Option[ActorRef] = None

  // XXX for monitoring...
  var longestReapDuration = 0L
  var lastReapDuration = 0L
  var currentLeaderElectedSince = 0L
  var commanderTimeoutCount = 0L
  var electedLeaderTimeoutCount = 0L
  var lastTimedOutPValue: Option[PValue] = None
  //TO-DO make configurable
  val defaultRetries = 2


  startScout()

  override def preStart() {
    registerMonitor(new LeaderInfo, config)
  }

  override def postStop() {
    unregisterMonitors(config)
  }

  def receive = {
    case propose @ Propose(slotNum, command) if !proposals.containsKey(slotNum) && slotNum > latestDecidedSlot =>
      electedLeader match {
        case Local =>
          proposals.put(slotNum, command)
          startCommander(PValue(myBallot, slotNum, command))

        case Remote(ref, _) =>
          ref forward propose

        case Unknown =>
          // stash for later
          proposals.put(slotNum, command)
      }

    // A majority of the Acceptors have accepted myBallot, become leader, stop watcher
    case Adopted(newBallot, pvals) if myBallot == newBallot =>
      logger.debug("Assuming leadership using {}", myBallot)

      // XXX: update actually has side effects, however this assignment
      //      is necessary for testing, we use it so that we can mock
      //      the leaderHelper without needing to use "andAnswer", or whatever.
      //      Eventually we should consider moving the leaderHelper stuff into
      //      the leader itself again...
      proposals = leaderHelper.update(proposals, leaderHelper.pmax(pvals))
      proposals.foreach(
        (slot, command) => startCommander(PValue(myBallot, slot, command))
      )
      currentLeaderElectedSince = System.currentTimeMillis()
      electedLeader = Local
      stopLeaderWatcher()


    // phantom ballot from the future- this node was the leader in some previous
    // life and other nodes still believe it, try to become leader again but using
    // a bigger ballot
    case Preempted(newBallot) if newBallot > myBallot && newBallot.leaderId == myLeaderId =>
      seekLeadership(newBallot)

    // there's a new leader, update electedLeaderBallot and start a new watcher accordingly
    case Preempted(newBallot) if newBallot > myBallot =>
      electedLeader match {
        case Remote(_, ballot) if ballot == newBallot =>
          // do nothing; duplicate Preempted message
        case _ =>
          handleLeaderChange(newBallot)
      }


    // try to become the new leader; old leader has gone MIA
    case LeaderGone =>
      electedLeaderTimeoutCount += 1
      electedLeader match {
        case Remote(_, ballot) => seekLeadership(ballot)
        case _ => seekLeadership(myBallot)
      }

    // respond to Ping from LeaderPinger with our current leader ballot information
    case Ping => electedLeader match {
      case Remote(_, ballot) => sender ! Pong(Some(ballot))
      case Local => sender ! Pong(Some(myBallot))
      case _ => sender ! Pong(None)
    }

    // if our scout fails to make progress, and we have not since elected a leader,
    //  try again
    case ScoutTimeout if electedLeader == Unknown => startScout()


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


    // the PaxosStateBridge will notify the Leader of the last decision.  We can then use this to reduce the number
    // of accepted decisions we need from the Acceptor
    case DecisionHint(lastSlot) =>
      latestDecidedSlot = lastSlot
      reapProposals()

    case Terminated(terminated) =>
      currentLeaderWatcher match {
        case Some(current) if current == terminated =>
          currentLeaderWatcher = None
        case _ =>
      }

  }

  private def handleLeaderChange(newLeaderBallot: Ballot) {
    stopLeaderWatcher()

    context.actorSelection(newLeaderBallot.leaderId).resolveOne(1 seconds) onComplete {
      case Success(actor) =>
        currentLeaderElectedSince = System.currentTimeMillis()
        electedLeader = Remote(actor, newLeaderBallot)
        proposals.foreach(
          (slot, command) => actor ! Propose(slot, command)
        )
        // XXX consider replacing with context.watch(actor) and dumping LeaderWatcher business...
        startLeaderWatcher()

      case Failure(_) =>
        startScout()
    }
  }

  private def startScout() {
    childProvider.createScout(self, membership.getClusterInfo, myBallot, latestDecidedSlot)
  }

  private def startCommander(pVal: PValue, ticks: Int = defaultRetries) {
    childProvider.createCommander(self, membership.getClusterInfo, pVal, ticks)
  }

  private def seekLeadership(ballotToTrump: Ballot) {
    myBallot = Ballot(ballotToTrump.seq + 1, myLeaderId)
    electedLeader = Unknown

    stopLeaderWatcher()
    startScout()
  }

  private def stopLeaderWatcher() {
    currentLeaderWatcher match {
      case Some(ref) => ref ! Close
      case _ => // no-op
    }
    currentLeaderWatcher = None
  }

  private def startLeaderWatcher() {
    stopLeaderWatcher()
    electedLeader match {
      case Remote(ref, ballot) =>
        val leaderWatcher = childProvider.createLeaderWatcher(ref, ballot, self)
        context.watch(leaderWatcher)
        currentLeaderWatcher = Some(leaderWatcher)
      case _ =>
    }
  }

  // drops all proposals held locally whose slot is <= latestDecidedSlot
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
    def getBallot: String
    def getLatestDecidedSlot: Long
    def getProposalCount: Int
    def getElectedLeaderBallot: String
    def getCurrentLeaderElectedSince: Long
    def getLongestReapDuration: Long
    def getLastReapDuration: Long
    def getCommanderTimeoutCount: Long
    def getLastTimedOutPValue: String
    def getLeaderWatcher: String
    def getElectedLeaderTimeoutCount: Long
  }

  class LeaderInfo extends LeaderInfoMBean {
    def getBallot = myBallot.toString
    def getLatestDecidedSlot = latestDecidedSlot
    def getProposalCount = proposals.size
    def getElectedLeaderBallot = electedLeader.toString
    def getCurrentLeaderElectedSince = currentLeaderElectedSince
    def getLongestReapDuration = longestReapDuration
    def getLastReapDuration = lastReapDuration
    def getCommanderTimeoutCount = commanderTimeoutCount
    def getLastTimedOutPValue = lastTimedOutPValue.toString
    def getLeaderWatcher = currentLeaderWatcher.toString
    def getElectedLeaderTimeoutCount = electedLeaderTimeoutCount
  }
}
