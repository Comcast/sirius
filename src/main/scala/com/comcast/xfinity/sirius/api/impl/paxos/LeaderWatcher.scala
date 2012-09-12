package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.{ActorRef, Props, Actor}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Preempted

object LeaderWatcher {
  // messages used in communication between Pinger and Watcher
  case object CheckLeader
  case object LeaderGone
  case class DifferentLeader(realBallot: Ballot)
  case object Close

  // messages sent up to Leader
  case object SeekLeadership
}

class LeaderWatcher(expectedBallot: Ballot, replyTo: ActorRef) extends Actor {
    import LeaderWatcher._

  // XXX configure from above
  val CHECK_FREQ_SEC = 5
  val checkCancellable =
    context.system.scheduler.schedule(0 seconds, CHECK_FREQ_SEC seconds, self, CheckLeader)

  def receive = {
    case CheckLeader =>
      createPinger(expectedBallot, self)

    case LeaderGone =>
      replyTo ! SeekLeadership
      context.stop(self)

    case DifferentLeader(realBallot: Ballot) =>
      replyTo ! Preempted(realBallot)
      context.stop(self)

    case Close =>
      context.stop(self)
  }

  private[paxos] def createPinger(expectedBallot: Ballot, replyTo: ActorRef): ActorRef =
    context.actorOf(Props(new LeaderPinger(expectedBallot, self)))

  override def postStop() {
    checkCancellable.cancel()
  }

}
