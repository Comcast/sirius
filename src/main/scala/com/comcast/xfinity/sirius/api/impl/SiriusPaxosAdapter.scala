package com.comcast.xfinity.sirius.api.impl

import akka.agent.Agent
import bridge.PaxosStateBridge
import paxos.PaxosMessages.Decision
import paxos.{Replica, PaxosSup}
import akka.actor.{ActorContext, Props, ActorRef}

object SiriusPaxosAdapter {

  /**
   * Oh hey I have an idea, I can refactor this and make it pretty and put it
   * inside of SiriusPaxosAdapter.  No, don't.  It appears that where this
   * appears inside of SiriusPaxosAdapter had some significance wrt initialization
   * order, so sometimes things got weird.
   *
   * This function creates the performFun that should be used by the Adapter,
   * pass in the paxosStateBridge you want to funnel events to, and it will
   * do the rest.
   *
   * Another interesting effect of having this out here is that the implicit
   * ActorContext inside of SiriusPaxosAdapter is no longer in scope, so the
   * sender here will be deadLetters, which should be completely fine.
   */
  def createPerformFun(paxosStateBridge: ActorRef): Replica.PerformFun =
    (d: Decision) => paxosStateBridge ! d
}

/**
 * Class responsible for adapting the Paxos subsystem for use in Sirius
 *
 * This class delegates all Decision handling to PaxosStateBridge, which is an
 * Actor.
 *
 * This class is more scaffolding than actual logic.
 *
 * This class must be created with an implicit ActorContext available, such
 * as from within an Actor.  On instantiation this context will be used to
 * create The Paxos subsystem and the stateBridge responsible for ordering
 * Paxos events for application to the persistence layer.
 *
 * @param membership the Agent[Set[ActorRef]] identifying cluster members
 * @param startingSeq the sequnce number to start with
 * @param persistenceActor the actor to send commited events to. This actor must
 *          know how to receive and understand
 * @param logRequestActor reference to actor that will handle paxosStateBridge's
 *          requests for log ranges.
 */
class SiriusPaxosAdapter(membership: Agent[Set[ActorRef]],
                         startingSeq: Long,
                         persistenceActor: ActorRef,
                         logRequestActor: ActorRef)(implicit context: ActorContext) {

  val paxosStateBridge = context.actorOf(Props(new PaxosStateBridge(startingSeq, persistenceActor, logRequestActor)), "paxos-state-bridge")

  val paxosSubSystem = context.actorOf(Props(
    PaxosSup(membership, startingSeq, SiriusPaxosAdapter.createPerformFun(paxosStateBridge))),
    "paxos"
  )
}