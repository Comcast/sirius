package com.comcast.xfinity.sirius.api.impl.bridge

import akka.actor.{ReceiveTimeout, ActorRef, Actor}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.OrderedEvent

object GapFetcher {
  // XXX extend some message type so the ChunkRequest can be routed to SiriusPersistenceActor
  case class Chunk(events: Seq[OrderedEvent])
  case class RequestChunk(startingSeq: Long, chunkSize: Int)

  def apply(firstGapSeq: Long, target: ActorRef, replyTo: ActorRef, config: SiriusConfiguration) {
    val chunkSize = config.getProp(SiriusConfiguration.LOG_REQUEST_CHUNK_SIZE, 1000)
    val chunkReceiveTimeout = config.getProp(SiriusConfiguration.LOG_REQUEST_RECEIVE_TIMEOUT_SECS, 5)

    new GapFetcher(firstGapSeq, target, replyTo, chunkSize, chunkReceiveTimeout)
  }
}

/**
 * Actor responsible for requesting gaps from a specified target, with no upper bound.  As long
 * as the target can fulfill the request, the actor will continue requesting.
 *
 * @param firstGapSeq first sequence number to request; lower bound
 * @param target actor from whom we should ask for chunks
 * @param replyTo actor to send OrderedEvents to
 * @param chunkSize number of events to request at a time
 * @param chunkReceiveTimeout how long to wait for events before stopping
 */
class GapFetcher(firstGapSeq: Long, target: ActorRef, replyTo: ActorRef, chunkSize: Int, chunkReceiveTimeout: Int)
                extends Actor {
    import GapFetcher._

  context.setReceiveTimeout(chunkReceiveTimeout seconds)

  var currentGapSeq = firstGapSeq
  requestChunk(firstGapSeq, chunkSize)

  def receive = {
    case chunk @ Chunk(events) if (events.last.sequence >= currentGapSeq + chunkSize - 1) =>
      processChunk(chunk, replyTo)
      currentGapSeq = currentGapSeq + chunkSize
      requestChunk(currentGapSeq, chunkSize)

    case chunk: Chunk =>
      processChunk(chunk, replyTo)
      context.stop(self)

    case ReceiveTimeout =>
      context.stop(self)
  }

  private[bridge] def requestChunk(currentGapSeq: Long, chunkSize: Int) {
    target ! RequestChunk(currentGapSeq, chunkSize)
  }

  private[bridge] def processChunk(chunk: Chunk, replyTo: ActorRef) {
    chunk.events.foreach(replyTo ! _)
  }
}
