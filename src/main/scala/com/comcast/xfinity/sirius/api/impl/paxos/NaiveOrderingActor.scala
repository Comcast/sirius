package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete, Put}
import com.comcast.xfinity.sirius.api.impl.NonCommutativeSiriusRequest
import akka.event.Logging
import akka.actor.{Actor, ActorRef}

/**
 * Actor for assigning order to sirius requests locally.
 *
 * This Actor only accepts {@link Put} and {@link Delete}
 * messages. Behaviour for other messages is undefined.
 *
 */
class NaiveOrderingActor(val persistenceActor: ActorRef) extends Actor {
  private val logger = Logging(context.system, this)

  var seq: Long = 0L


  def receive = {
    case put: Put => processRequest(put)
    case delete: Delete => processRequest(delete)
    case _ =>
      logger.warning("NaiveOrderingActor only accepts PUT's and DELETE's and PaxosMessages")

  }

  private def processRequest(req: NonCommutativeSiriusRequest) {
    seq = seq + 1
    persistenceActor forward OrderedEvent(seq, System.currentTimeMillis(), req)

  }

}
