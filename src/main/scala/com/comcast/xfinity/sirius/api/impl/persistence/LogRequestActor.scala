package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.{Props, ActorRef, Actor}
import scala.None
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.membership._
import com.comcast.xfinity.sirius.writeaheadlog.LogIteratorSource
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.admin.MonitoringHooks


sealed trait LogRequestMessage
case class RequestLogFromRemote(remote: ActorRef, logRange: LogRange, targetActor: ActorRef) extends LogRequestMessage
case class RequestLogFromAnyRemote(logRange: LogRange, targetActor: ActorRef) extends LogRequestMessage
case class InitiateTransfer(receiver: ActorRef, logRange: LogRange) extends LogRequestMessage
case class TransferComplete(duration: Long) extends LogRequestMessage
case class TransferFailed(reason: String) extends LogRequestMessage

object LogRequestActor {
  val NO_MEMBER_FAIL_MSG = "Could not get remote node to request logs from"
}

/**
 * Actor responsible for handling requests around remote log bootstrapping.
 * @param chunkSize number of log events to ship at once
 * @param source LogIteratorSource for sequential reading of the source log
 * @param membershipAgent agent holding current membership information
 */
class LogRequestActor(chunkSize: Int, source: LogIteratorSource,
      localSiriusRef: ActorRef, membershipAgent: Agent[Set[ActorRef]])
      (implicit config: SiriusConfiguration = new SiriusConfiguration) extends Actor with MonitoringHooks {

  override def preStart() {
    registerMonitor(new LogRequestActorInfo, config)
  }
  override def postStop() {
    unregisterMonitors(config)
  }

  // XXX for monitoring...
  var lastTransferDuration = 0L

  def membershipHelper = new MembershipHelper

  def createSender(): ActorRef =
    context.actorOf(Props(new LogSendingActor))

  def createReceiver(targetActor: ActorRef): ActorRef =
    context.actorOf(Props(new LogReceivingActor(targetActor)))

  protected def receive = {
    case RequestLogFromRemote(remote, logRange, targetActor) =>
      val receiver = createReceiver(targetActor)
      remote ! InitiateTransfer(receiver, logRange)

    case RequestLogFromAnyRemote(logRange, targetActor) => {
      val membershipData = membershipHelper.getRandomMember(membershipAgent(), localSiriusRef)
      membershipData match {
        case None => context.parent ! TransferFailed(LogRequestActor.NO_MEMBER_FAIL_MSG)
        case Some(member) => self ! RequestLogFromRemote(member, logRange, targetActor)
      }
    }

    case InitiateTransfer(receiver, logRange) =>
      val logSender = createSender()
      logSender ! Start(receiver, source, logRange, chunkSize)

    case TransferComplete(duration) =>
      lastTransferDuration = duration
      context.parent ! TransferComplete
  }

  /**
   * Monitoring hooks
   */
  trait LogRequestActorInfoMBean {
    def getLastTransferDuration: Long
  }

  class LogRequestActorInfo extends LogRequestActorInfoMBean {
    def getLastTransferDuration = lastTransferDuration
  }
}
