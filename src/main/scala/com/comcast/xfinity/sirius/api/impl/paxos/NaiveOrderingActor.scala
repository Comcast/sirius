package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete, Put}
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest
import akka.event.Logging
import akka.actor.{Actor, ActorRef}
import com.comcast.xfinity.sirius.api.SiriusResult

/**
 * Actor for assigning order to sirius requests locally.
 *
 * This Actor only accepts {@link Put} and {@link Delete}
 * messages. Behaviour for other messages is undefined.
 *
 */
class NaiveOrderingActor(val persistenceActor: ActorRef, var nextSeq: Long) extends Actor {
  private val logger = Logging(context.system, "Sirius")

  def receive = {
    case put: Put => processRequest(put)
    case delete: Delete => processRequest(delete)
    case _ =>
      logger.warning("NaiveOrderingActor can only accept PUT and DELETE requests")

  }

  private def processRequest(req: NonCommutativeSiriusRequest) {
    persistenceActor forward OrderedEvent(nextSeq, System.currentTimeMillis(), req)
    nextSeq = nextSeq + 1
    //XXX: return as soon as ordering is complete
    sender ! SiriusResult.none

  }

}
