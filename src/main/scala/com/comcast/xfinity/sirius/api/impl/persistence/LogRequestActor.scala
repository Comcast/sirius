package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.{Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.writeaheadlog.LogLinesSource
import com.comcast.xfinity.sirius.api.impl.membership.{MemberInfo, GetRandomMember}
import scala.None

sealed trait LogRequestMessage
case class RequestLogFromRemote(remote: ActorRef) extends LogRequestMessage
case class InitiateTransfer(receiver: ActorRef) extends LogRequestMessage
case object TransferComplete extends LogRequestMessage
case class TransferFailed(reason: String) extends LogRequestMessage

object LogRequestActor {
  val NO_MEMBER_FAIL_MSG = "Could not get remote node to request logs from"
}
class LogRequestActor(chunkSize: Int, source: LogLinesSource) extends Actor {

  def createSender(): ActorRef =
    context.actorOf(Props(new LogSendingActor))

  def createReceiver(): ActorRef =
    context.actorOf(Props(new LogReceivingActor))

  protected def receive = {
    case RequestLogFromRemote(remote) =>
      val receiver = createReceiver()
      remote ! InitiateTransfer(receiver)

    case MemberInfo(None) =>
      context.parent ! TransferFailed(LogRequestActor.NO_MEMBER_FAIL_MSG)

    case MemberInfo(Some(member)) =>
      self ! RequestLogFromRemote(member.supervisorRef)

    case RequestLogFromRemote =>
      context.parent ! GetRandomMember

    case InitiateTransfer(receiver) =>
      val logSender = createSender()
      logSender ! Start(receiver, source, chunkSize)

    case TransferComplete =>
      context.parent ! TransferComplete
  }
}
