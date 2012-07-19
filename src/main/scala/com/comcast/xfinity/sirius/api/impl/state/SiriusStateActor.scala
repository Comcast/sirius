package com.comcast.xfinity.sirius.api.impl.state

import com.comcast.xfinity.sirius.api.RequestHandler
import akka.actor.Actor
import com.comcast.xfinity.sirius.api.impl._
import akka.agent.Agent

/**
 * Actor responsible for a applying changes to our in memory state.  It does this by
 * delegating to the passed in {@link RequestHandler}.
 */
class SiriusStateActor(val requestHandler: RequestHandler, siriusStateAgent: Agent[SiriusState])
  extends Actor {

  override def preStart() {
    siriusStateAgent send ((state: SiriusState) => {
          state.updateStateActorState(SiriusState.StateActorState.Initialized)
    })
  }
  
  def receive = {
    case Get(key) => sender ! requestHandler.handleGet(key)
    case Delete(key) => sender ! requestHandler.handleDelete(key)
    case Put(key, body) => sender ! requestHandler.handlePut(key, body)
  }
}