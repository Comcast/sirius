package com.comcast.xfinity.sirius.api.impl.paxos

import akka.agent.Agent
import akka.actor.{Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

class PaxosSup(membership: Agent[Set[ActorRef]]) extends Actor {
  val leader = context.actorOf(Props(new Leader(membership)))
  val acceptor = context.actorOf(Props(new Acceptor))
  val replica = context.actorOf(Props(new Replica(membership)))

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