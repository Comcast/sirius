package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.{Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.writeaheadlog.LogIteratorSource
import com.comcast.xfinity.sirius.api.impl.membership.{MembershipHelper, MembershipMap}
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.agent.Agent
import scala.None

sealed trait LogRequestMessage
case class RequestLogFromRemote(remote: ActorRef) extends LogRequestMessage
case object RequestLogFromAnyRemote extends LogRequestMessage
case class InitiateTransfer(receiver: ActorRef) extends LogRequestMessage
case object TransferComplete extends LogRequestMessage
case class TransferFailed(reason: String) extends LogRequestMessage

object LogRequestActor {
  val NO_MEMBER_FAIL_MSG = "Could not get remote node to request logs from"
}

/**
 * Actor responsible for handling requests around remote log bootstrapping.
 * @param chunkSize number of log events to ship at once
 * @param source LogIteratorSource for sequential reading of the source log
 * @param persistenceActor persistence actor, for forwarding received log data
 */
class LogRequestActor(chunkSize: Int, source: LogIteratorSource,
      siriusInfo: SiriusInfo, persistenceActor: ActorRef, membershipAgent: Agent[MembershipMap]) extends Actor {

  def membershipHelper = new MembershipHelper

  def createSender(): ActorRef =
    context.actorOf(Props(new LogSendingActor))

  def createReceiver(): ActorRef =
    context.actorOf(Props(new LogReceivingActor(persistenceActor)))

  protected def receive = {
    case RequestLogFromRemote(remote) =>
      val receiver = createReceiver()
      remote ! InitiateTransfer(receiver)

    case RequestLogFromAnyRemote => {
      val membershipData = membershipHelper.getRandomMember(membershipAgent.get(), siriusInfo)
      membershipData match {
        case None => context.parent ! TransferFailed(LogRequestActor.NO_MEMBER_FAIL_MSG)
        case Some(member) => self ! RequestLogFromRemote(member.supervisorRef)
      }
    }

    case InitiateTransfer(receiver) =>
      val logSender = createSender()
      logSender ! Start(receiver, source, chunkSize)

    case TransferComplete =>
      context.parent ! TransferComplete
  }
}
