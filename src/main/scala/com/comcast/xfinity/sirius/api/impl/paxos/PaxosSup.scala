package com.comcast.xfinity.sirius.api.impl.paxos

import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest
import akka.event.Logging

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
   * @param startingSeqNum the sequence number at which this node will begin issuing/acknowledging
   * @param performFun function specified by
   *          [[com.comcast.xfinity.sirius.api.impl.paxos.Replica.PerformFun]], applied to
   *          decisions as they arrive
   */
  def apply(membership: Agent[Set[ActorRef]], startingSeqNum: Long, performFun: Replica.PerformFun): PaxosSup = {
    val REPLICA_REAP_WINDOW_MS = 10000
    val REPLICA_REAP_SCHEDULE_FREQ_SEC = 1
    new PaxosSup with ChildProvider {
      val leader = context.actorOf(Props(Leader(membership, startingSeqNum)), "leader")
      val acceptor = context.actorOf(Props(Acceptor(startingSeqNum)), "acceptor")
      val replica = context.actorOf(Props(Replica(leader, startingSeqNum, performFun,
                                                  REPLICA_REAP_WINDOW_MS,
                                                  REPLICA_REAP_SCHEDULE_FREQ_SEC)), "replica")
    }
  }
}

class PaxosSup extends Actor {
  this: PaxosSup.ChildProvider =>

  val traceLog = Logging(context.system, "SiriusTrace")

  def receive = {
    // Replica messages
    case GetLowestUnusedSlotNum => replica forward GetLowestUnusedSlotNum
    case req: NonCommutativeSiriusRequest =>
      traceLog.debug("Received event for submission {}", req)
      val command = Command(sender, System.currentTimeMillis(), req)
      replica forward Request(command)
    case d: Decision => replica forward d

    // Leader messages
    case p: Propose => leader forward p
    case dh: DecisionHint => leader forward  dh
    // Adopted and Preempted are internal
    // Acceptor messages
    case p1a: Phase1A => acceptor forward p1a
    case p2A: Phase2A => acceptor forward p2A
    // Phase1B and Phase2B are direct addressed
  }
}
