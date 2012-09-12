package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.{ReceiveTimeout, ActorRef, Actor}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.{DifferentLeader, LeaderGone}

object LeaderPinger {
  case object Ping
  case class Pong(leaderBallot: Option[Ballot])
}

class LeaderPinger(expectedBallot: Ballot, replyTo: ActorRef) extends Actor {
    import LeaderPinger._

  // XXX configurable from above
  val PING_RECEIVE_TIMEOUT = 2
  context.setReceiveTimeout(PING_RECEIVE_TIMEOUT seconds)

  val expectedLeader = context.actorFor(expectedBallot.leaderId)

  expectedLeader ! Ping

  def receive = {
    // we had the wrong guy; our expectedLeader wrong; tell dad the real truth
    case Pong(Some(leaderBallot)) if (leaderBallot != expectedBallot) =>
      replyTo ! DifferentLeader(leaderBallot)
      context.stop(self)

    case Pong(Some(_)) =>
      context.stop(self)

    case Pong(None) =>
      replyTo ! LeaderGone
      context.stop(self)

    case ReceiveTimeout =>
      replyTo ! LeaderGone
      context.stop(self)
  }
}
