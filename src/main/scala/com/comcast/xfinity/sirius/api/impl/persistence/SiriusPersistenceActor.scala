package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.writeaheadlog.{LogData, LogWriter}
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete, Put}

/**
 * {@link Actor} for persisting data to the write ahead log and forwarding
 * to the state worker.
 */
class SiriusPersistenceActor(val stateWorker: ActorRef, logWriter: LogWriter) extends Actor {
  def receive = {
    case event: OrderedEvent => {
      event.request match {
        case put: Put => logWriter.writeEntry(LogData("PUT", put.key, event.sequence, event.timestamp, Some(put.body)))
        case delete: Delete => logWriter.writeEntry(LogData("DELETE", delete.key, event.sequence, event.timestamp, None))
      }
      stateWorker forward event.request
    }
  }
}