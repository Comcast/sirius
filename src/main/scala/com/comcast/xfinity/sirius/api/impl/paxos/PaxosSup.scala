package com.comcast.xfinity.sirius.api.impl.paxos

import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{ActorSystem, Props, ActorRef, Actor}
import java.lang.Object


object PaxosSup {

  /**
   * A class for injecting children into a PaxosSup
   */
  trait ChildProvider {
    val leader: ActorRef
    val acceptor: ActorRef
    val replica: ActorRef
  }

  /**
   * Create a PaxosSup instance.  Note this should be called from within a Props
   * factory on Actor creation due to the requirements of Akka.
   *
   * @param membership an {@link akka.agent.Agent} tracking the membership of the cluster
   */
  def apply(membership: Agent[Set[ActorRef]]): PaxosSup = {
    new PaxosSup with ChildProvider {
      val leader = context.actorOf(Props(Leader(membership)), "leader")
      val acceptor = context.actorOf(Props(new Acceptor), "acceptor")
      val replica = context.actorOf(Props(Replica(membership)), "replica")
    }
  }

}

class PaxosSup extends Actor {
    this: PaxosSup.ChildProvider =>

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