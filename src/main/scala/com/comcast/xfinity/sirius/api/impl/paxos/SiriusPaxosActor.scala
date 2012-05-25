package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, SiriusRequest, Delete, Put}
import org.slf4j.LoggerFactory
import com.comcast.xfinity.sirius.api.impl.NonIdempotentSiriusRequest

/**
 * Actor for negotiating Paxos rounds locally.
 *
 * For now this Actor only accepts {@link Put} and {@link Get}
 * messages. Behaviour for other messages is undefined.
 *
 * TODO: Currently this is just a placeholder for the Paxos layer,
 *       here to draw a cutoff point
 */
class SiriusPaxosActor(val persistenceActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger(classOf[SiriusPaxosActor])

  var seq: Long = 0L

  def receive = {
    case put: Put => processRequest(put)
    case delete: Delete => processRequest(delete)
    case _ =>
      logger.warn("SiriusPaxosActor only accepts PUT's and DELETE's")

  }

  private def processRequest(req: NonIdempotentSiriusRequest) = {
    seq = seq + 1
    persistenceActor forward OrderedEvent(seq, System.currentTimeMillis(), req)
  }
}