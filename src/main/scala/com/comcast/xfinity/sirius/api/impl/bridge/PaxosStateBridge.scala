package com.comcast.xfinity.sirius.api.impl.bridge

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import collection.SortedMap
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.{Command, Decision}
import com.comcast.xfinity.sirius.api.SiriusResult
import annotation.tailrec
import akka.actor.{Actor, ActorRef}

/**
 * Actor responsible for bridging the gap between the Paxos layer and
 * the persistence layer.
 *
 * This Actor contains the necessary logic for assuring that events are only
 * applied to the persistence layer in order.  As designed currently (on
 * purpose) the Paxos system will blindly deliver decisions, even if they have
 * already been decided.  This allows nodes that are behind to catch up.  Also,
 * there is no gaurentee that events will arrive in order, so a later event
 * may arrive before a current event.
 *
 * To accomplish this we buffer events that come before their time, only keeping
 * the first copy of each.
 *
 * XXX: in its current form it does not wait for confirmation that an event has been
 *   committed to disk by the persistence layer, we should really add that, but for
 *   now this should be good enough.
 *
 * @param startingSeq the sequence number to start with
 * @param stateSup reference to the subsystem encapsulating system state.
 *            In the future as the code settles down we may want to have this
 *            directly point to the persistence layer, but for now we funnel
 *            everything through the state subsystem supervisor for abstraction,
 *            such that we can easily refactor and not worry about messing stuff
 *            up.
 */
class PaxosStateBridge(startingSeq: Long,
                       stateSup: ActorRef) extends Actor {

  var nextSeq: Long = startingSeq
  var eventBuffer = SortedMap[Long, OrderedEvent]()

  def receive = {
    /*
     * When a decision arrives for the first time the actor identified by
     * Decision.command.client is sent a SiriusResult.none message to aknowledge the
     * event has been ordered. The OrderedEvent contained in the Decision is then
     * buffered to be sent to the persistence layer.  All ready decisions (contiguous
     * ones starting with the current sequence number) are sent to the persistence
     * layer to be written to disk and memory and are dropped from the buffer.
     */
    case Decision(slot, Command(client, ts, op)) if slot >= nextSeq && !eventBuffer.contains(slot) =>
      eventBuffer += (slot -> OrderedEvent(slot, ts, op))
      client ! SiriusResult.none()
      executeReadyDecisions()
  }

  /**
   * With side effects, applies all decisions in the local queue ready for application.  The
   * side effects are that the queue is trimmed and nextSeq is updated.
   */
  private def executeReadyDecisions() {
    @tailrec
    def applyNextReadyDecision() {
      eventBuffer.headOption match {
        case Some((slot, orderedEvent)) if slot == nextSeq =>
          stateSup ! orderedEvent
          nextSeq += 1
          eventBuffer = eventBuffer.tail
          applyNextReadyDecision()
        case _ =>
      }
    }
    applyNextReadyDecision()
  }
}