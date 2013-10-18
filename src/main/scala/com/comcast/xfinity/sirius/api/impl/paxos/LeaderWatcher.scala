package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.{ActorContext, ActorRef, Props, Actor}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Preempted
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.ChildProvider

object LeaderWatcher {
  // messages used in communication between Pinger and Watcher
  trait LeaderWatcherMessage
  case object CheckLeader extends LeaderWatcherMessage
  case object LeaderGone extends LeaderWatcherMessage
  case class LeaderPong(rtt: Long) extends LeaderWatcherMessage
  case class DifferentLeader(realBallot: Ballot) extends LeaderWatcherMessage

  // message received from the Leader
  case object Close

  // messages sent up to Leader
  case object SeekLeadership

  trait LeaderWatcherInfoMBean {
    def getLastPingRTT: Option[Long]
    def getTimeSinceLastPong: Option[Long]
  }

  /**
   * Factory for creating the children actors of LeaderWatcher.
   *
   * @param config the SiriusConfiguration for this node
   */
  class ChildProvider(config: SiriusConfiguration) {
    val PING_TIMEOUT_MS = config.getProp(SiriusConfiguration.PAXOS_LEADERSHIP_PING_TIMEOUT, 2000)

    private[paxos] def createPinger(expectedBallot: Ballot, replyTo: ActorRef)
                                   (implicit context: ActorContext): ActorRef =
      context.actorOf(LeaderPinger.props(expectedBallot, replyTo, PING_TIMEOUT_MS))
  }

  /**
   * Create Props for a LeaderWatcher actor.
   *
   * @param ballotToWatch currently elected ballot to watch
   * @param leader actorRef to the local leader
   * @param config the SiriusConfiguration for this node
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(ballotToWatch: Ballot, leader: ActorRef, config: SiriusConfiguration): Props = {
    val childProvider = new ChildProvider(config)
    //Props(classOf[LeaderWatcher], ballotToWatch, leader, childProvider, config)
    Props(new LeaderWatcher(ballotToWatch, leader, childProvider, config))
  }
}

class LeaderWatcher(expectedBallot: Ballot, replyTo: ActorRef, childProvider: ChildProvider,
                    config: SiriusConfiguration)
      extends Actor with MonitoringHooks {
    import LeaderWatcher._

  val CHECK_FREQ_SEC = config.getProp(SiriusConfiguration.PAXOS_LEADERSHIP_PING_INTERVAL, 20)
  val checkCancellable =
    context.system.scheduler.schedule(0 seconds, CHECK_FREQ_SEC seconds, self, CheckLeader)

  // statistics for JMX
  var lastPingRTT: Option[Long] = None
  var lastPingTime: Option[Long] = None

  def receive = {
    case CheckLeader =>
      childProvider.createPinger(expectedBallot, self)

    case LeaderGone =>
      replyTo ! SeekLeadership
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
