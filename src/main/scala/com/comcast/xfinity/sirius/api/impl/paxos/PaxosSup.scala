package com.comcast.xfinity.sirius.api.impl.paxos

import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.actor.{ActorContext, Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest
import akka.event.Logging
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosSup.ChildProvider

object PaxosSup {

  /**
   * Factory for creating children actors of PaxosSup.
   *
   * @param membership an {@link akka.agent.Agent} tracking the membership of the cluster
   * @param startingSeq the sequence number at which this node will begin issuing/acknowledging
   * @param performFun function specified by
   *          [[com.comcast.xfinity.sirius.api.impl.paxos.Replica.PerformFun]], applied to
   *          decisions as they arrive
   * @param config SiriusConfiguration object for configuring children actors.
   */
  protected[paxos] class ChildProvider(membership: Agent[Set[ActorRef]], startingSeq: Long, performFun: Replica.PerformFun, config: SiriusConfiguration) {
    def createLeader()(implicit context: ActorContext) =
      context.actorOf(Props(Leader(membership, startingSeq, config)), "leader")

    def createAcceptor()(implicit context: ActorContext) =
      context.actorOf(Props(Acceptor(startingSeq, config)), "acceptor")

    def createReplica(leader: ActorRef)(implicit context: ActorContext) =
      context.actorOf(Props(Replica(leader, startingSeq, performFun, config)), "replica")
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
  def apply(membership: Agent[Set[ActorRef]],
            startingSeqNum: Long,
            performFun: Replica.PerformFun,
            config: SiriusConfiguration): PaxosSup = {
    new PaxosSup(new ChildProvider(membership, startingSeqNum, performFun, config))
  }
}

// TODO rename this PaxosSupervisor
class PaxosSup(childProvider: ChildProvider) extends Actor {

  val leader = childProvider.createLeader
  val acceptor = childProvider.createAcceptor
  val replica = childProvider.createReplica(leader)

  val traceLog = Logging(context.system, "SiriusTrace")

  def receive = {
    // Replica messages
    case req: NonCommutativeSiriusRequest =>
      traceLog.debug("Received event for submission {}", req)
      val command = Command(sender, System.currentTimeMillis(), req)
      replica forward Request(command)
    case d: Decision => replica forward d
    case dh: DecisionHint => replica forward  dh

    // Leader messages
    case p: Propose => leader forward p
    // Adopted and Preempted are internal
    // Acceptor messages
    case p1a: Phase1A => acceptor forward p1a
    case p2A: Phase2A => acceptor forward p2A
    // Phase1B and Phase2B are direct addressed
  }
}
