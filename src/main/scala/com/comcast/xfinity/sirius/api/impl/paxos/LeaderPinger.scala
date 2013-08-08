package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.{ReceiveTimeout, ActorRef, Actor}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.{LeaderPong, DifferentLeader, LeaderGone}

object LeaderPinger {
  case object Ping
  case class Pong(leaderBallot: Option[Ballot])
}

class LeaderPinger(expectedBallot: Ballot, replyTo: ActorRef, timeoutMs: Int) extends Actor {
    import LeaderPinger._

  context.setReceiveTimeout(timeoutMs milliseconds)

  val expectedLeader = context.actorFor(expectedBallot.leaderId)
  val pingSent = System.currentTimeMillis()

  expectedLeader ! Ping

  def receive = {
    case Pong(Some(leaderBallot)) if leaderBallot != expectedBallot =>
      replyTo ! DifferentLeader(leaderBallot)
      context.stop(self)

    case Pong(Some(_)) =>
      replyTo ! LeaderPong(System.currentTimeMillis() - pingSent)
      context.stop(self)

    case Pong(None) =>
      replyTo ! LeaderGone
      context.stop(self)

    case ReceiveTimeout =>
      replyTo ! LeaderGone
      context.stop(self)
  }
}
