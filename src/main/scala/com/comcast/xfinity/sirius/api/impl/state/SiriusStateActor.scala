/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.api.impl.state

import akka.actor.{Props, Actor}
import com.comcast.xfinity.sirius.api.impl._
import com.comcast.xfinity.sirius.api.{SiriusResult, RequestHandler}
import akka.event.Logging
import com.comcast.xfinity.sirius.api.impl.SiriusRequest

object SiriusStateActor {

  /**
   * Create Props for a SiriusStateActor.
   *
   * @param requestHandler the RequestHandler containing the callbacks for manipulating this instance's state
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(requestHandler: RequestHandler): Props = {
    Props(classOf[SiriusStateActor], requestHandler)
  }
}
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
