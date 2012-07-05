package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.Actor
import org.slf4j.LoggerFactory
import com.comcast.xfinity.sirius.writeaheadlog.{WriteAheadLogSerDe, LogDataSerDe}

class LogReceivingActor extends Actor {
  private val logger = LoggerFactory.getLogger(classOf[LogSendingActor])
  // private val serializer: WriteAheadLogSerDe = new WriteAheadLogSerDe()

  def receive = {

    // TODO should keep track of current chunkNum to ensure in-order delivery and enable resends
    case LogChunk(chunkNum: Int, chunk: Seq[String]) =>
      sender ! Received(chunkNum)

      // TODO deserialize lines in chunk and send to PersistenceActor
      /*
      chunk.map((data) => {
        val logData = serializer.deserialize(data)
      })
       */

      logger.debug("Received " + chunk.size + " lines")
      sender ! Processed(chunkNum)

    case DoneMsg =>
      logger.debug("Received done message")
      sender ! DoneAck
      context.parent ! TransferComplete
  }
}
