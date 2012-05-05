package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.RequestMethod

import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.dispatch.Await

/**
 * A Sirius implementation implemented in Scala using Akka actors
 */
class SiriusScalaImpl(val requestHandler: RequestHandler, val actorSystem: ActorSystem) extends AkkaConfig {

  val stateWorker = actorSystem.actorOf(Props(new SiriusStateWorker(requestHandler)), SIRIUS_STATE_WORKER_NAME)

  /**
   * Enqueue an event for processing
   */
  def enqueue(method: RequestMethod, key: String, body: Array[Byte]) = {
    val akkaFuture = stateWorker ?(method, key, body)
    Await.result(akkaFuture, timeout.duration).asInstanceOf[Array[Byte]]

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