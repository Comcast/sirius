package com.comcast.xfinity.sirius.api.impl.state

import com.comcast.xfinity.sirius.api.RequestHandler
import akka.agent.Agent
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import akka.actor.{Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.api.impl._
import akka.event.Logging
import state.SiriusPersistenceActor.LogQuery

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

      val logger = Logging(context.system, "Sirius")

      // this stuff is essentially just autowiring, but we may want to move the contents
      //  of this wiring to a top level-ish class for style
      val start = System.currentTimeMillis
      logger.info("Beginning SiriusLog replay at {}", start)
      bootstrapState(requestHandler, siriusLog)
      logger.info("Replayed SiriusLog in {}ms", System.currentTimeMillis - start)

      val stateActor =
        context.actorOf(Props(new SiriusStateActor(requestHandler, siriusStateAgent)), "memory_state")

      val persistenceActor =
        context.actorOf(Props(new SiriusPersistenceActor(stateActor, siriusLog, siriusStateAgent)), "persistence")
    }
  }

  /**
   * Replay siriusLog into requestHandler (has side effects!)
   *
   * @param requestHandler the RequestHandler to replay into
   * @param siriusLog the SiriusLog to replay from
   */
  def bootstrapState(requestHandler: RequestHandler, siriusLog: SiriusLog) {
    // Perhaps we should think about adding the foreach abstraction back to SiriusLog
    siriusLog.foldLeft(()) {
      case (acc, OrderedEvent(_, _, Put(key, body))) => requestHandler.handlePut(key, body); acc
      case (acc, OrderedEvent(_, _, Delete(key))) => requestHandler.handleDelete(key); acc
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

    case logQuery: LogQuery =>
      persistenceActor forward logQuery

  }
}