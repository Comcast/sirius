package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.writeaheadlog.{LogData, SiriusLog}
import com.comcast.xfinity.sirius.api.SiriusResult
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl._
import akka.event.Logging

/**
 * {@link Actor} for persisting data to the write ahead log and forwarding
 * to the state worker.
 */
class SiriusPersistenceActor(val stateActor: ActorRef, siriusLog: SiriusLog, siriusStateAgent: Agent[SiriusState])
  extends Actor {

  val logger = Logging(context.system, this)

  override def preStart() {

    logger.info("Bootstrapping Write Ahead Log")
    val start = System.currentTimeMillis()
    siriusLog.foldLeft(()) {
      case (_, LogData("PUT", key, _, _, Some(body))) => {
        logger.debug("Read PUT from log: key={} --- body={}", key, body);
        stateActor ! Put(key, body)
      }
      case (_, LogData("DELETE", key, _, _, _)) => {
        logger.debug("Read DELETE from log: key={}", key);
        stateActor ! Delete(key)
      }
    }
    logger.info("Done Bootstrapping Write Ahead Log in {} ms", System.currentTimeMillis() - start)
    siriusStateAgent send ((state: SiriusState) => {
      state.updatePersistenceState(SiriusState.PersistenceState.Initialized)
    })

  }

  def receive = {
    case OrderedEvent(sequence, timestamp, request) => {
      request match {
        case Put(key, body) => siriusLog.writeEntry(LogData("PUT", key, sequence, timestamp, Some(body)))
        case Delete(key) => siriusLog.writeEntry(LogData("DELETE", key, sequence, timestamp, None))
      }
      stateActor forward request
    }
    case _: SiriusResult =>
  }
}