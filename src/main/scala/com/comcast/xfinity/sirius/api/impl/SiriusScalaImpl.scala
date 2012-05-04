package com.comcast.xfinity.sirius.api.impl
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.RequestMethod
import com.comcast.xfinity.sirius.api.Sirius

import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.duration._

/**
 * A Sirius implementation implemented in Scala using Akka actors
 */
class SiriusScalaImpl(val requestHandler: RequestHandler, val actorSystem: ActorSystem) {
  
  val stateWorker = actorSystem.actorOf(Props(new SiriusStateWorker(requestHandler)))
   
  /**
   * Enqueue an event for processing
   */
  def enqueue(method: RequestMethod, key: String, body: Array[Byte]) = {
    val akkaFuture = ask(stateWorker, (method, key, body))(5 seconds)
    akkaFuture.asInstanceOf[Future[Array[Byte]]]
  }
  
  /**
   * Enqueue a PUT for processing
   */
  def enqueuePut(key: String, body: Array[Byte]) = enqueue(RequestMethod.PUT, key, body)
  
  /**
   * Enqueue a GET for processing
   */
  def enqueueGet(key: String) = enqueue(RequestMethod.GET, key, null)
}