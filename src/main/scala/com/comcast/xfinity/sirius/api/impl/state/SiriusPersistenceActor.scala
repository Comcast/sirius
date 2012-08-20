package com.comcast.xfinity.sirius.api.impl.state

import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.SiriusResult
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl._

/**
 * {@link Actor} for persisting data to the write ahead log and forwarding
 * to the state worker.
 *
 * Events must arrive in order, this actor does not enforce such, but the underlying
 * SiriusLog may.
 *
 * @param stateActor Actor wrapping in memory state of the system
 * @param siriusLog the log to record events to before sending to memory
 * @param siriusStateAgent agent containing an administrative state of the system to update
 *            once bootstrapping has completed
 */
class SiriusPersistenceActor(val stateActor: ActorRef, siriusLog: SiriusLog, siriusStateAgent: Agent[SiriusState])
  extends Actor {

  override def preStart() {
    siriusStateAgent send ((state: SiriusState) => {
      state.updatePersistenceState(SiriusState.PersistenceState.Initialized)
    })
  }

  def receive = {
    case event: OrderedEvent =>
      siriusLog.writeEntry(event)
      stateActor ! event.request

    // SiriusStateActor responds to writes with a SiriusResult, we don't really want this
    // anymore, and it should be eliminated in a future commit
    case _: SiriusResult =>
  }

}