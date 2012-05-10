package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.RequestMethod
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.dispatch.Await
import com.comcast.xfinity.sirius.api.Sirius
import akka.dispatch.Future

/**
 * A Sirius implementation implemented in Scala using Akka actors
 */
class SiriusImpl(val requestHandler: RequestHandler, val actorSystem: ActorSystem) extends Sirius with AkkaConfig {

  val stateWorker = actorSystem.actorOf(Props(new SiriusStateWorker(requestHandler)), SIRIUS_STATE_WORKER_NAME)

  /**
   * ${@inheritDoc}
   */
  def enqueuePut(key: String, body: Array[Byte]) = {
    (stateWorker ? Put(key, body)).asInstanceOf[Future[Array[Byte]]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueGet(key: String) = {
    (stateWorker ? Get(key)).asInstanceOf[Future[Array[Byte]]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueDelete(key: String) = {
    (stateWorker ? Delete(key)).asInstanceOf[Future[Array[Byte]]]
  }
}