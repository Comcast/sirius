package com.comcast.xfinity.sirius.api.impl.paxos

import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest

object PaxosSup {

  /**
   * Case class for submitting a request for ordering to the Paxos
   * subsystem
   *
   * @param client the initiating ActorRef
   * @param req the request to submit
   */
  case class Submit(req: NonCommutativeSiriusRequest)

  /**
   * A class for injecting children into a PaxosSup
   */
  trait ChildProvider {
    val leader: ActorRef
    val acceptor: ActorRef
    val replica: ActorRef
  }

  /**
   * Same as the 3 arg constructor, except defaults startingSeqNum to 1
   */
  @deprecated
  def apply(membership: Agent[Set[ActorRef]], performFun: Replica.PerformFun): PaxosSup =
    apply(membership, 1, performFun)

  /**
   * Create a PaxosSup instance.  Note this should be called from within a Props
   * factory on Actor creation due to the requirements of Akka.
   *
   * @param membership an {@link akka.agent.Agent} tracking the membership of the cluster
   * @param startingSeqNum the sequence number at which this node will begin issuing/acknowledging
   * @param performFun function specified by
   *          [[com.comcast.xfinity.sirius.api.impl.paxos.Replica.PerformFun]], applied to
   *          decisions as they arrive
   */
  def apply(membership: Agent[Set[ActorRef]], startingSeqNum: Long, performFun: Replica.PerformFun): PaxosSup = {
    new PaxosSup with ChildProvider {
      val leader = context.actorOf(Props(Leader(membership, startingSeqNum)), "leader")
      val acceptor = context.actorOf(Props(Acceptor(startingSeqNum)), "acceptor")
      val replica = context.actorOf(Props(Replica(membership, startingSeqNum, performFun)), "replica")
    }
  }
}

class PaxosSup extends Actor {
  this: PaxosSup.ChildProvider =>

  def receive = {
    // Replica messages
    case PaxosSup.Submit(req) =>
      val command = Command(sender, System.currentTimeMillis(), req)
      replica forward Request(command)
    case r: Request => replica forward r // <-- not used, to be removed later
    case d: Decision => replica forward d

    // Leader messages
    case p: Propose => leader forward p
    // Adopted and Preempted are internal
    // Acceptor messages
    case p1a: Phase1A => acceptor forward p1a
    case p2A: Phase2A => acceptor forward p2A
    // Phase1B and Phase2B are direct addressed
  }
}
