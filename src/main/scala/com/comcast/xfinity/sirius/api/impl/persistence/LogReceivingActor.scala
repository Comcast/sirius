package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.{ActorRef, Actor}
import org.slf4j.LoggerFactory
import com.comcast.xfinity.sirius.writeaheadlog.{LogData, LogDataSerDe}
import com.comcast.xfinity.sirius.api.impl.{Delete, Put, NonIdempotentSiriusRequest, OrderedEvent}

/**
 * Actor that catches chunks of logs, usually sent by a LogSendingActor, then deserializes
 * them and sends them to the PersistenceActor to be written to mem/disk.
 * @param persistenceActor ref to actor that persists updates
 * @param logSerializer serializer/deserializer, used to turn log lines to LogData
 */
class LogReceivingActor(persistenceActor: ActorRef, logSerializer: LogDataSerDe) extends Actor {
  private val logger = LoggerFactory.getLogger(classOf[LogSendingActor])

  def getSiriusRequestFromLogData(logData: LogData): NonIdempotentSiriusRequest = {
    val actionType = logData.actionType
    val key = logData.key
    val body = logData.payload

    actionType.toUpperCase match {
      case "PUT" =>
        Put(key, body.get)
      case "DELETE" =>
        Delete(key)
    }
  }

  def receive = {

    // TODO should keep track of current chunkNum to ensure in-order delivery and enable resends
    case LogChunk(chunkNum, chunk) =>
      sender ! Received(chunkNum)

      chunk.map((data) => {
        val logData = logSerializer.deserialize(data)
        val event = OrderedEvent(logData.sequence, logData.timestamp, getSiriusRequestFromLogData(logData))
        persistenceActor ! event
      })

      logger.debug("Received " + chunk.size + " lines")
      sender ! Processed(chunkNum)

    case DoneMsg =>
      logger.debug("Received done message")
      sender ! DoneAck
      context.parent ! TransferComplete
  }
}
