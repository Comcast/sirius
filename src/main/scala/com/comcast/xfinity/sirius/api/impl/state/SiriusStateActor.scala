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

  val logger = Logging(context.system, "Sirius")

  override def preStart() {
    // XXX: do we really need this...
    siriusStateAgent send (_.copy(stateInitialized = true))
  }
  
  def receive = {
    case req: SiriusRequest =>
      // XXX: With the way things work now, we probably shouldn't
      //      be responding to Puts and Deletes...  These are
      //      responded to when an order has been decided on
      sender ! processSiriusRequestSafely(req)
  }

  private def processSiriusRequestSafely(req: SiriusRequest): SiriusResult = {
    try {
      req match {
        case Get(key) => requestHandler.handleGet(key)
        case Delete(key) => requestHandler.handleDelete(key)
        case Put(key, body) => requestHandler.handlePut(key, body)
      }
    } catch {
      case e: RuntimeException =>
        logger.error("Unhandled exception in handling {}: {}", req, e)
        SiriusResult.error(e)
    }
  }
}