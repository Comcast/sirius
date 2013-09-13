package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.{ActorRef, Props, Actor}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Preempted
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.api.SiriusConfiguration

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
}

class LeaderWatcher(expectedBallot: Ballot, replyTo: ActorRef)
                   (implicit config: SiriusConfiguration = new SiriusConfiguration)
      extends Actor with MonitoringHooks {
    import LeaderWatcher._

  // XXX get from config
  val CHECK_FREQ_SEC = config.getProp(SiriusConfiguration.PAXOS_LEADERSHIP_PING_INTERVAL, 20)
  val PING_TIMEOUT_MS = config.getProp(SiriusConfiguration.PAXOS_LEADERSHIP_PING_TIMEOUT, 2000)
  val checkCancellable =
    context.system.scheduler.schedule(0 seconds, CHECK_FREQ_SEC seconds, self, CheckLeader)

  // statistics for JMX
  var lastPingRTT: Option[Long] = None
  var lastPingTime: Option[Long] = None

  def receive = {
    case CheckLeader =>
      createPinger(expectedBallot, self)

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

  private[paxos] def createPinger(expectedBallot: Ballot, replyTo: ActorRef): ActorRef =
    context.actorOf(Props(new LeaderPinger(expectedBallot, self, PING_TIMEOUT_MS)))

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
