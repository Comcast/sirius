package com.comcast.xfinity.sirius.api.impl.bridge

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import collection.SortedMap
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.SiriusResult
import annotation.tailrec
import akka.actor.{Actor, ActorRef}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.persistence.{RequestLogFromAnyRemote, BoundedLogRange}

object PaxosStateBridge {
  object RequestGaps
}

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
 * @param logRequestActor reference to actor that will handle requests for log ranges.
 */
class PaxosStateBridge(startingSeq: Long,
                       stateSup: ActorRef,
                       logRequestActor: ActorRef) extends Actor {
    import PaxosStateBridge._

  // TODO: make this configurable- it's cool hardcoded for now, but once
  //       SiriusConfig matures this would be pretty clean to configure
  val requestGapsCancellable =
    context.system.scheduler.schedule(60 seconds, 60 seconds, self, RequestGaps)

  var nextSeq: Long = startingSeq
  var eventBuffer = SortedMap[Long, OrderedEvent]()
  var unreadyDecisionCnt:Int = 0

  override def postStop() { requestGapsCancellable.cancel() }

  def receive = {

    case UnreadyDecisionsCountReq =>sender ! UnreadyDecisionCount(unreadyDecisionCnt)

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
      unreadyDecisionCnt = unreadyDecisionCnt+1
      client ! SiriusResult.none()
      executeReadyDecisions()
    case RequestGaps =>
      requestGaps()
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
          unreadyDecisionCnt = unreadyDecisionCnt - 1
          applyNextReadyDecision()
        case _ =>
      }
    }
    applyNextReadyDecision()
  }

  /**
   * Given the current nextSeq and eventBuffer, finds current gaps that are preventing persistence
   * and requests them from the local logRequestActor.  Does not modify nextSeq or eventBuffer.
   */
  private def requestGaps() {
    val gaps = findAllGaps(nextSeq, eventBuffer)
    // XXX: requesting multiple gaps can become quite costly given the current implementation
    //      of log shipping, requesting multiple distinct chunks can become costly, especially
    //      if those chunks are out of window
    gaps.foreach(
      (br: BoundedLogRange) =>
        logRequestActor ! RequestLogFromAnyRemote(br, self)
    )
  }

  /**
   * Generate list of "gaps" in an event buffer.  Gaps are ranges of sequence numbers that:
   * - do not appear in the buffer
   * - are preventing later writes from being persisted
   *
   * @param nextSeqExpected next expected sequence number
   * @param buffer buffer to search
   * @param accum accumulator, defaults to empty if not provided
   * @return list of bounded log ranges representing gaps.
   */
  @tailrec
  private def findAllGaps(nextSeqExpected: Long,
                          buffer: SortedMap[Long, OrderedEvent],
                          accum: List[BoundedLogRange] = Nil): List[BoundedLogRange] = {
    buffer.headOption match {
      case Some((seq, event)) if seq > nextSeqExpected =>
        findAllGaps(seq + 1, buffer.tail, accum :+ BoundedLogRange(nextSeqExpected, seq - 1))
      case Some((seq, event)) =>
        findAllGaps(seq + 1, buffer.tail, accum)
      case None =>
        accum
    }
  }
}
