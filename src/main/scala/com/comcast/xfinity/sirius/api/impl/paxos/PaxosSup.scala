package com.comcast.xfinity.sirius.api.impl.paxos

import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest
import akka.event.Logging

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
  @deprecated("instead use 3 arg constructor","8-15-12 at least")
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
      val replica = context.actorOf(Props(Replica(leader, startingSeqNum, performFun)), "replica")
    }
  }
}

class PaxosSup extends Actor {
  this: PaxosSup.ChildProvider =>

  val log = Logging(context.system, this)

  def receive = {
    // Replica messages
    case GetLowestUnusedSlotNum => replica forward GetLowestUnusedSlotNum
    case PaxosSup.Submit(req) =>
      log.debug("Received event for submission {}", req)
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
