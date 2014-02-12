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

import akka.actor.{ActorContext, ActorRef, Props, Actor}
import scala.concurrent.duration._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Preempted
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.ChildProvider
import scala.language.postfixOps

object LeaderWatcher {
  // messages used in communication between Pinger and Watcher
  trait LeaderWatcherMessage
  case object CheckLeader extends LeaderWatcherMessage
  case object LeaderGone extends LeaderWatcherMessage
  case class LeaderPong(rtt: Long) extends LeaderWatcherMessage
  case class DifferentLeader(realBallot: Ballot) extends LeaderWatcherMessage

  // message received from the Leader
  case object Close

  trait LeaderWatcherInfoMBean {
    def getLastPingRTT: Option[Long]
    def getTimeSinceLastPong: Option[Long]
  }

  /**
   * Factory for creating the children actors of LeaderWatcher.
   *
   * @param leaderToWatch ref for currently elected leader to keep an eye on
   * @param expectedBallot currently elected ballot to watch
   * @param config the SiriusConfiguration for this node
   */
  class ChildProvider(leaderToWatch: ActorRef, expectedBallot: Ballot, config: SiriusConfiguration) {
    val PING_TIMEOUT_MS = config.getProp(SiriusConfiguration.PAXOS_LEADERSHIP_PING_TIMEOUT, 2000)

    private[paxos] def createPinger(replyTo: ActorRef)
                                   (implicit context: ActorContext): ActorRef =
      context.actorOf(LeaderPinger.props(leaderToWatch: ActorRef, expectedBallot, replyTo, PING_TIMEOUT_MS))
  }

  /**
   * Create Props for a LeaderWatcher actor.
   *
   * @param leaderToWatch ref for currently elected leader to keep an eye on
   * @param ballotToWatch currently elected ballot to watch
   * @param leader actorRef to the local leader
   * @param config the SiriusConfiguration for this node
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(leaderToWatch: ActorRef, ballotToWatch: Ballot, leader: ActorRef, config: SiriusConfiguration): Props = {
    val childProvider = new ChildProvider(leaderToWatch, ballotToWatch, config)
    Props(classOf[LeaderWatcher], leader, childProvider, config)
  }
}

// XXX consider replacing this whole jawn with context.watch(leaderRef), akka does most of this for us
class LeaderWatcher(replyTo: ActorRef, childProvider: ChildProvider,
                    config: SiriusConfiguration)
      extends Actor with MonitoringHooks {
    import LeaderWatcher._

  implicit val executionContext = context.system.dispatcher

  val CHECK_FREQ_SEC = config.getProp(SiriusConfiguration.PAXOS_LEADERSHIP_PING_INTERVAL, 20)
  val checkCancellable =
    context.system.scheduler.schedule(0 seconds, CHECK_FREQ_SEC seconds, self, CheckLeader)

  // statistics for JMX
  var lastPingRTT: Option[Long] = None
  var lastPingTime: Option[Long] = None

  def receive = {
    case CheckLeader =>
      childProvider.createPinger(self)

    case LeaderGone =>
      replyTo ! LeaderGone
      context.stop(self)

    case DifferentLeader(realBallot: Ballot) =>
      replyTo ! Preempted(realBallot)
      context.stop(self)

    case LeaderPong(rtt) =>
      lastPingRTT = Some(rtt)
      lastPingTime = Some(System.currentTimeMillis())

    case Close =>
      context.stop(self)
  }

  override def preStart() {
    registerMonitor(new LeaderWatcherInfo, config)
  }

  override def postStop() {
    unregisterMonitors(config)
    checkCancellable.cancel()
  }

  class LeaderWatcherInfo extends LeaderWatcherInfoMBean {
    def getLastPingRTT = lastPingRTT
    def getTimeSinceLastPong = lastPingTime match {
      case Some(time) => Some(System.currentTimeMillis() - time)
      case None => None
    }
  }

}
