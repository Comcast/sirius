package com.comcast.xfinity.sirius.api.impl.paxos

import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{ActorSystem, Props, ActorRef, Actor}

object PaxosSup {

  def apply(name: String, membership: Agent[Set[ActorRef]])(implicit as: ActorSystem) =
    as.actorOf(Props(new PaxosSup(membership)), name)

}

class PaxosSup(membership: Agent[Set[ActorRef]]) extends Actor {
  val leader = context.actorOf(Props(new Leader(membership)), "leader")
  val acceptor = context.actorOf(Props(new Acceptor), "acceptor")
  val replica = context.actorOf(Props(new Replica(membership)), "replica")

  def receive = {
    // Replica messages
    case r: Request => replica forward r
    case d: Decision => replica forward d

    // Leader messages
    case p: Propose => leader forward  p
    // Adopted and Preempted are internal

    // Acceptor messages
    case p1a: Phase1A => acceptor forward p1a
    case p2A: Phase2A => acceptor forward p2A
    // Phase1B and Phase2B are direct addressed
  }
}