package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.Actor
import org.slf4j.LoggerFactory

class LogReceivingActor extends Actor {
  private val logger = LoggerFactory.getLogger(classOf[LogSendingActor])

  def receive = {

    // should keep track of current seqNum to ensure in-order delivery and enable resends
    case LogChunk(sequenceNum, chunk) =>
      sender ! Received(sequenceNum)
      logger.debug("Received " + chunk.size + " lines")
      sender ! Processed(sequenceNum)

    case DoneMsg =>
      logger.debug("Received done message")
      sender ! DoneAck
  }
}
