package com.comcast.xfinity.sirius.api.impl.state

import akka.actor.Actor
import com.comcast.xfinity.sirius.api.impl._
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.{SiriusResult, RequestHandler}
import akka.event.Logging
import com.comcast.xfinity.sirius.api.impl.SiriusRequest

/**
 * Actor responsible for a applying changes to our in memory state.  It does this by
 * delegating to the passed in {@link RequestHandler}.  The result of calling the
 * associated method on the passed in RequestHandler is sent back to the sender.
 *
 * In the future we may want to not respond to Put and Delete messages...
 *
 * @param requestHandler the request handler containing callbacks for manipulating state
 * @param siriusStateAgent the Agent to update once we have successfully initialized. This
 *              parameter may be removed in the future in favor of a different startup style
 */
class SiriusStateActor(requestHandler: RequestHandler,
                       siriusStateAgent: Agent[SiriusState]) extends Actor {

  val log = Logging(context.system, this)

  override def preStart() {
    siriusStateAgent send ((state: SiriusState) => {
          state.updateStateActorState(SiriusState.StateActorState.Initialized)
    })
  }
  
  def processSiriusRequest(req: SiriusRequest) {
    val result = try {
      req match {
        case Get(key) => requestHandler.handleGet(key)
        case Delete(key) => requestHandler.handleDelete(key)
        case Put(key, body) => requestHandler.handlePut(key, body)
      }
    } catch {
      case t: Throwable =>
        log.error("Unhandled exception in handling {}: {}", req, t)
        SiriusResult.error(t)
    }
    sender ! result
  }

  def receive = {
    case req: SiriusRequest => processSiriusRequest(req)
  }
}