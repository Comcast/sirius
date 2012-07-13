package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.{Props, ActorRef, Actor}
import scala.None
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.membership._
import com.comcast.xfinity.sirius.writeaheadlog.LogIteratorSource


sealed trait LogRequestMessage
case class RequestLogFromRemote(remote: ActorRef, logRange: LogRange) extends LogRequestMessage
case class RequestLogFromAnyRemote(logRange: LogRange) extends LogRequestMessage
case class InitiateTransfer(receiver: ActorRef, logRange: LogRange) extends LogRequestMessage
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
      siriusId: String, persistenceActor: ActorRef, membershipAgent: Agent[MembershipMap]) extends Actor {

  def membershipHelper = new MembershipHelper

  def createSender(): ActorRef =
    context.actorOf(Props(new LogSendingActor))

  def createReceiver(): ActorRef =
    context.actorOf(Props(new LogReceivingActor(persistenceActor)))

  protected def receive = {
    case RequestLogFromRemote(remote, logRange) =>
      val receiver = createReceiver()
      remote ! InitiateTransfer(receiver, logRange)

    case RequestLogFromAnyRemote(logRange) => {
      val membershipData = membershipHelper.getRandomMember(membershipAgent.get(), siriusId)
      membershipData match {
        case None => context.parent ! TransferFailed(LogRequestActor.NO_MEMBER_FAIL_MSG)
        case Some(member) => self ! RequestLogFromRemote(member.supervisorRef, logRange)
      }
    }

    case InitiateTransfer(receiver, logRange) =>
      val logSender = createSender()
      logSender ! Start(receiver, source, logRange, chunkSize)

    case TransferComplete =>
      context.parent ! TransferComplete
  }
}
