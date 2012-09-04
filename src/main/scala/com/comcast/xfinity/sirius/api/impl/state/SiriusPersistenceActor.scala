package com.comcast.xfinity.sirius.api.impl.state

import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.SiriusResult
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl._
import persistence.BoundedLogRange

object SiriusPersistenceActor {

  /**
   * Message for directly requesting a chunk of the log from a node.
   *
   * SiriusPersistenceActor is expected to reply with a LogSubrange
   * when receiving this message.  This range should be as complete
   * as possible.
   *
   * @param begin first sequence number of the range
   * @param end last sequence number of the range, inclusive
   */
  case class GetLogSubrange(begin: Long, end: Long)

  /**
   * Message encapsulating a range of events, as asked for by
   * a GetSubrange message.
   *
   * @param events the OrderedEvents in this range, in order
   */
  case class LogSubrange(events: List[OrderedEvent])
}

/**
 * {@link Actor} for persisting data to the write ahead log and forwarding
 * to the state worker.
 *
 * Events must arrive in order, this actor does not enforce such, but the underlying
 * SiriusLog may.
 *
 * @param stateActor Actor wrapping in memory state of the system
 * @param siriusLog the log to record events to before sending to memory
 * @param siriusStateAgent agent containing an administrative state of the system to update
 *            once bootstrapping has completed
 */
class SiriusPersistenceActor(val stateActor: ActorRef, siriusLog: SiriusLog, siriusStateAgent: Agent[SiriusState])
    extends Actor {

  import SiriusPersistenceActor._

  override def preStart() {
    siriusStateAgent send ((state: SiriusState) => {
      state.updatePersistenceState(SiriusState.PersistenceState.Initialized)
    })
  }

  def receive = {
    case event: OrderedEvent =>
      siriusLog.writeEntry(event)
      stateActor ! event.request

    // XXX: cap max request chunk size hard coded at 1000 for now for sanity
    case GetLogSubrange(begin, end) if begin <= end && (begin - end) < 1000 =>
      val chunkRange = siriusLog.createIterator(BoundedLogRange(begin, end))
      sender ! LogSubrange(chunkRange.toList)

    // SiriusStateActor responds to writes with a SiriusResult, we don't really want this
    // anymore, and it should be eliminated in a future commit
    case _: SiriusResult =>
  }

}