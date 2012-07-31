package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.SiriusResult
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl._
import akka.event.Logging

/**
 * {@link Actor} for persisting data to the write ahead log and forwarding
 * to the state worker.
 */
class SiriusPersistenceActor(val stateActor: ActorRef, siriusLog: SiriusLog, siriusStateAgent: Agent[SiriusState])
  extends Actor {

  val logger = Logging(context.system, this)

  override def preStart() {

    logger.info("Bootstrapping Write Ahead Log")
    val start = System.currentTimeMillis()
    // XXX: replace accum Unit with Any? then we don't have to worry
    //      about returning a unit in the end
    siriusLog.foldLeft(Unit)((acc, orderedEvent) => {
      stateActor ! orderedEvent.request
      acc
    })
    logger.info("Done Bootstrapping Write Ahead Log in {} ms", System.currentTimeMillis() - start)
    siriusStateAgent send ((state: SiriusState) => {
      state.updatePersistenceState(SiriusState.PersistenceState.Initialized)
    })

  }

  def receive = {
    case event: OrderedEvent => {
      siriusLog.writeEntry(event)
      stateActor forward event.request
    }
    case _: SiriusResult =>
  }
}