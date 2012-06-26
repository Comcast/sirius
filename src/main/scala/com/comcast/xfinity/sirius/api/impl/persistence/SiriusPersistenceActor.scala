package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.writeaheadlog.{LogData, SiriusLog}
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete, Put}
import com.comcast.xfinity.sirius.api.SiriusResult

/**
 * {@link Actor} for persisting data to the write ahead log and forwarding
 * to the state worker.
 */
class SiriusPersistenceActor(val stateActor: ActorRef, siriusLog: SiriusLog) extends Actor {
  
  override def preStart() {
    siriusLog.foldLeft(()){
      case (_, LogData("PUT", key, _, _, Some(body))) => stateActor ! Put(key, body)
      case (_, LogData("DELETE", key, _, _, _)) => stateActor ! Delete(key)
    }
  }
  
  def receive = {
    case OrderedEvent(sequence, timestamp, request) => {
      request match {
        case Put(key, body) => siriusLog.writeEntry(LogData("PUT", key, sequence, timestamp, Some(body)))
        case Delete(key) => siriusLog.writeEntry(LogData("DELETE", key, sequence, timestamp, None))
      }
      stateActor forward request
    }
    case _ : SiriusResult =>
  }
}