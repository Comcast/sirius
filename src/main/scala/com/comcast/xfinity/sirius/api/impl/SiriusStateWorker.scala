package com.comcast.xfinity.sirius.api.impl
import com.comcast.xfinity.sirius.api.RequestHandler
import akka.actor.Actor
import com.comcast.xfinity.sirius.api.RequestMethod

/**
 * Actor wrapping a {@link RequestHandler} for single threaded, actor like access
 */
class SiriusStateWorker(val requestHandler: RequestHandler) extends Actor {
  
  def receive = {
    case (requestMethod: RequestMethod, key: String, body: Array[Byte]) =>
      sender ! requestHandler.handle(requestMethod, key, body)
  }
}