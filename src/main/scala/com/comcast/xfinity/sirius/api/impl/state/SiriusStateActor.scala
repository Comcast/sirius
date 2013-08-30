package com.comcast.xfinity.sirius.api.impl.state

import akka.actor.Actor
import com.comcast.xfinity.sirius.api.impl._
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
 */
class SiriusStateActor(requestHandler: RequestHandler) extends Actor {

  val logger = Logging(context.system, "Sirius")
  
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
        logger.warning("Unhandled exception in handling {}: {}", req, e)
        SiriusResult.error(e)
    }
  }
}