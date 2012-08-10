package com.comcast.xfinity.sirius.api.impl.state

import com.comcast.xfinity.sirius.api.RequestHandler
import akka.agent.Agent
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Get, SiriusState}
import akka.actor.{Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.api.impl.persistence.SiriusPersistenceActor

object StateSup {
  trait ChildProvider {
    val stateActor: ActorRef
    val persistenceActor: ActorRef
  }

  /**
   * Create a StateSup managing the state of requestHandler and persisting data to siriusLog.
   * As this instantiates an actor, it must be called via the Props factory object using actorOf.
   *
   * @param requestHandler the RequestHandler to apply updates to and perform Gets on
   * @param siriusLog the SiriusLog to persist OrderedEvents to
   * @param siriusStateAgent agent containing information on the state of the system.
   *            This should eventually go bye bye
   *
   * @returns the StateSup managing the state subsystem.
   */
  def apply(requestHandler: RequestHandler,
            siriusLog: SiriusLog,
            siriusStateAgent: Agent[SiriusState]): StateSup = {
    new StateSup with ChildProvider {
      val stateActor =
        context.actorOf(Props(new SiriusStateActor(requestHandler, siriusStateAgent)), "memory_state")

      val persistenceActor =
        context.actorOf(Props(new SiriusPersistenceActor(stateActor, siriusLog, siriusStateAgent)), "persistence")
    }
  }
}

/**
 * Actors for supervising state related matters
 */
class StateSup extends Actor {
    this: StateSup.ChildProvider =>

  def receive = {
    case get: Get =>
      stateActor forward get

    case orderedEvent: OrderedEvent =>
      persistenceActor ! orderedEvent

  }
}