package com.comcast.xfinity.sirius.api.impl.paxos

import akka.util.duration._
import akka.actor.{Props, ReceiveTimeout, ActorRef, Actor}
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.{LeaderPong, DifferentLeader, LeaderGone}

object LeaderPinger {
  case object Ping
  case class Pong(leaderBallot: Option[Ballot])

  /**
   * Create Props for a LeaderPinger actor.
   *
   * @param expectedBallot last-known "elected" leader ballot
   * @param replyTo actor to inform about elected leader state
   * @param timeoutMs how long to wait for a response before declaring leader dead
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(expectedBallot: Ballot, replyTo: ActorRef, timeoutMs: Int): Props = {
    //Props(classOf[LeaderPinger], expectedBallot, replyTo, timeoutMs)
    Props(new LeaderPinger(expectedBallot, replyTo, timeoutMs))
  }
}

private[paxos] class LeaderPinger(expectedBallot: Ballot, replyTo: ActorRef, timeoutMs: Int) extends Actor {
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
