package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.{ActorRef, Actor}
import org.slf4j.LoggerFactory

/**
 * Actor that catches chunks of logs, usually sent by a LogSendingActor, then deserializes
 * them and sends them to the PersistenceActor to be written to mem/disk.
 * @param persistenceActor ref to actor that persists updates
 */
class LogReceivingActor(persistenceActor: ActorRef) extends Actor {
  private val logger = LoggerFactory.getLogger(classOf[LogSendingActor])
  private val startTime = System.currentTimeMillis()
  private var numLinesReceived = 0

  def receive = {
    // TODO should keep track of current chunkNum to ensure in-order delivery and enable resends
    case LogChunk(chunkNum, chunk) =>
      // XXX: may be able to omit this?
      sender ! Received(chunkNum)

      // XXX: assuming a Seq is an ordered collection?
      chunk.foreach(persistenceActor ! _)

      numLinesReceived += chunk.size
      logger.debug("Received " + chunk.size + " events")
      sender ! Processed(chunkNum)

    // XXX: do we need a way to time out one of these transactions? else we may wind up with
    //      leaked actors, and lots of them
    case DoneMsg =>
      logger.debug("Received done message")
      logger.info("Received {} events in {} ms", numLinesReceived, System.currentTimeMillis() - startTime)
      sender ! DoneAck
      context.parent ! TransferComplete
      context.stop(self)
  }
}
