package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.{Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.writeaheadlog.{WriteAheadLogSerDe, LogDataSerDe, LogLinesSource}
import com.comcast.xfinity.sirius.api.impl.membership.{MemberInfo, GetRandomMember}
import scala.None

sealed trait LogRequestMessage
case class RequestLogFromRemote(remote: ActorRef) extends LogRequestMessage
case object RequestLogFromRemote extends LogRequestMessage
case class InitiateTransfer(receiver: ActorRef) extends LogRequestMessage
case object TransferComplete extends LogRequestMessage
case class TransferFailed(reason: String) extends LogRequestMessage

object LogRequestActor {
  val NO_MEMBER_FAIL_MSG = "Could not get remote node to request logs from"
}

/**
 * Actor responsible for handling requests around remote log bootstrapping.
 * @param chunkSize number of log lines to ship at once
 * @param source LogLinesSource for sequential reading of the source log
 * @param persistenceActor persistence actor, for forwarding received log data
 */
class LogRequestActor(chunkSize: Int, source: LogLinesSource, persistenceActor: ActorRef) extends Actor {
  val serializer: LogDataSerDe = new WriteAheadLogSerDe()

  def createSender(): ActorRef =
    context.actorOf(Props(new LogSendingActor))

  def createReceiver(): ActorRef =
    context.actorOf(Props(new LogReceivingActor(persistenceActor, serializer)), "logreceiver")

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
