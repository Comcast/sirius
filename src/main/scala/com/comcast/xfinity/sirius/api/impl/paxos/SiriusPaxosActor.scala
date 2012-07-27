package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete, Put}
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest
import akka.agent.Agent
import akka.event.Logging
import scala.Predef._
import akka.actor.{Props, Actor, ActorRef}
import org.apache.commons.lang.NotImplementedException

/**
 * Actor for negotiating Paxos rounds locally.
 *
 * For now this Actor only accepts {@link Put} and {@link Delete}
 * messages. Behaviour for other messages is undefined.
 *
 * TODO: Currently this is just a placeholder for the Paxos layer,
 *       here to draw a cutoff point
 */
class SiriusPaxosActor(val persistenceActor: ActorRef, membershipAgent: Agent[Set[ActorRef]]) extends Actor {
  private val logger = Logging(context.system, this)

  var seq: Long = 0L

  val performDecision = (slot: Long, request: NonCommutativeSiriusRequest) => {
    persistenceActor forward OrderedEvent(seq, System.currentTimeMillis(), request)
    true
  }

  private val paxosSupervisor = createPaxosSupervisor(membershipAgent, performDecision)

  def receive = {

    case _ =>
      throw new NotImplementedException("Paxos is not implemented .... fool.")

  }

  private def processRequest(req: NonCommutativeSiriusRequest) {

    //TODO: wire in paxos components when fully implemented
  }


  def createPaxosSupervisor(memAgent: Agent[Set[ActorRef]],
                            perfDec: Replica.PerformFun): ActorRef = {

    context.actorOf(Props(PaxosSup(memAgent, perfDec)), "paxos-supervisor")
  }

}
