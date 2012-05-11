package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.api.Sirius
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.Future
import akka.pattern.ask
import com.comcast.xfinity.sirius.api.impl.state.SiriusStateActor

/**
 * A Sirius implementation implemented in Scala using Akka actors
 */
class SiriusImpl(val requestHandler: RequestHandler, val actorSystem: ActorSystem) extends Sirius with AkkaConfig {

  val stateActor = actorSystem.actorOf(Props(new SiriusStateActor(requestHandler)), SIRIUS_STATE_WORKER_NAME)

  /**
   * ${@inheritDoc}
   */
  def enqueuePut(key: String, body: Array[Byte]) = {
    (stateActor ? Put(key, body)).asInstanceOf[Future[Array[Byte]]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueGet(key: String) = {
    (stateActor ? Get(key)).asInstanceOf[Future[Array[Byte]]]
  }

  /**
   * ${@inheritDoc}
   */
  def enqueueDelete(key: String) = {
    (stateActor ? Delete(key)).asInstanceOf[Future[Array[Byte]]]
  }
}