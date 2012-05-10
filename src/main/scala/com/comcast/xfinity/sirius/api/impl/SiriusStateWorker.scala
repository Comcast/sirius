package com.comcast.xfinity.sirius.api.impl
import com.comcast.xfinity.sirius.api.RequestHandler
import akka.actor.Actor
import com.comcast.xfinity.sirius.api.RequestMethod

/**
 * Actor wrapping a {@link RequestHandler} for single threaded, actor like access
 */
class SiriusStateWorker(val requestHandler: RequestHandler) extends Actor {
  
  def receive = {
    case Get(key) => sender ! requestHandler.handleGet(key)
    case Delete(key) => sender ! requestHandler.handleDelete(key)
    case Put(key, body) => sender ! requestHandler.handlePut(key, body)
  }
}