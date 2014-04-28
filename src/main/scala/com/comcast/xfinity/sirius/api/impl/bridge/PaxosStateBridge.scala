/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.api.impl.bridge

import com.comcast.xfinity.sirius.api.{SiriusConfiguration, SiriusResult}
import annotation.tailrec
import akka.actor._
import scala.concurrent.duration._
import akka.event.Logging
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import com.comcast.xfinity.sirius.util.RichJTreeMap
import com.comcast.xfinity.sirius.api.impl.bridge.PaxosStateBridge.{ChildProvider, RequestGaps}
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.LogSubrange
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.DecisionHint
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Decision
import com.comcast.xfinity.sirius.api.impl.bridge.PaxosStateBridge.RequestFromSeq
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Command
import scala.language.postfixOps

object PaxosStateBridge {
  case object RequestGaps
  case class RequestFromSeq(begin: Long)

  /**
   * Factory for creating the children actors of PaxosStateBridge.
   *
   * @param config the SiriusConfiguration for this node
   */
  class ChildProvider(config: SiriusConfiguration) {
    def createGapFetcher(seq: Long, target: ActorRef, requester: ActorRef)(implicit context: ActorContext) = {
      context.actorOf(GapFetcher.props(seq, target, requester, config))
    }
  }

  /**
   * Create Props for a PaxosStateBridge actor.
   *
   * @param startingSeq the sequence number to start with
   * @param stateSupActor reference to the subsystem encapsulating system state.
   * @param siriusSupActor reference to the Sirius Supervisor Actor for routing
   *          DecisionHints to the Paxos Subsystem
   * @param membershipHelper reference to object that knows how to get a random
   *                         remote cluster member
   * @param config SiriusConfiguration for this node
   * @return Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(startingSeq: Long,
            stateSupActor: ActorRef,
            siriusSupActor: ActorRef,
            membershipHelper: MembershipHelper,
            config: SiriusConfiguration): Props = {
    val childProvider = new ChildProvider(config)
    Props(classOf[PaxosStateBridge], startingSeq, stateSupActor, siriusSupActor, membershipHelper, childProvider, config)
  }
}

/**
 * Actor responsible for bridging the gap between the Paxos layer and
 * the persistence layer.
 *
 * This Actor contains the necessary logic for assuring that events are only
 * applied to the persistence layer in order.  As designed currently (on
 * purpose) the Paxos system will blindly deliver decisions, even if they have
 * already been decided.  This allows nodes that are behind to catch up.  Also,
 * there is no guarantee that events will arrive in order, so a later event
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
 * @param stateSupActor reference to the subsystem encapsulating system state.
 *            In the future as the code settles down we may want to have this
 *            directly point to the persistence layer, but for now we funnel
 *            everything through the state subsystem supervisor for abstraction,
 *            such that we can easily refactor and not worry about messing stuff
 *            up.
 * @param siriusSupActor reference to the Sirius Supervisor Actor for routing
 *          DecisionHints to the Paxos Subsystem
 * @param membershipHelper reference to object that knows how to get a random
 *                         remote cluster member
 */
class PaxosStateBridge(startingSeq: Long,
                       stateSupActor: ActorRef,
                       siriusSupActor: ActorRef,
                       membershipHelper: MembershipHelper,
                       childProvider: ChildProvider,
                       config: SiriusConfiguration)
      extends Actor with MonitoringHooks {

  implicit val executionContext = context.system.dispatcher

  val chunkSize = config.getProp(SiriusConfiguration.LOG_REQUEST_CHUNK_SIZE, 1000)

  val logger = Logging(context.system, "Sirius")
  val traceLogger = Logging(context.system, "SiriusTrace")

  var gapFetcher: Option[ActorRef] = None

  val requestGapsFreq = config.getProp(SiriusConfiguration.LOG_REQUEST_FREQ_SECS, 30)
  val requestGapsCancellable =
    context.system.scheduler.schedule(requestGapsFreq seconds, requestGapsFreq seconds, self, RequestGaps)

  var nextSeq: Long = startingSeq
  var eventBuffer = RichJTreeMap[Long, OrderedEvent]()

  // monitor stats, for original catchup duration
  var startupCatchupDuration: Option[Long] = None
  val startupTimestamp = System.currentTimeMillis()

  logger.info("Starting PaxosStateBridge with nextSeq {}, attempting catch up every {}s",
    nextSeq, requestGapsFreq)

  override def preStart() {
    registerMonitor(new PaxosStateBridgeInfo, config)
  }
  override def postStop() {
    unregisterMonitors(config)
    requestGapsCancellable.cancel()
  }

  def receive = {

    /*
     * When a decision arrives for the first time the actor identified by
     * Decision.command.client is sent a SiriusResult.none message to acknowledge the
     * event has been ordered. The OrderedEvent contained in the Decision is then
     * buffered to be sent to the persistence layer.  All ready decisions (contiguous
     * ones starting with the current sequence number) are sent to the persistence
     * layer to be written to disk and memory and are dropped from the buffer.
     */
    case Decision(slot, Command(client, ts, op)) if slot >= nextSeq && !eventBuffer.containsKey(slot) =>
      processOrderedEvent(OrderedEvent(slot, ts, op))
      client ! SiriusResult.none()
      traceLogger.debug("Time to respond to client for sequence={}: {}ms", slot, System.currentTimeMillis() - ts)

    /**
     * When receiving a LogSubrange, we need to check to see whether it's useful.  These
     * messages originate from the GapFetcher, which proactively seeks out new updates
     * that might be locally missed.  If we receive this message and it's helpful, indicate
     * to the GapFetcher that we're ready for more.  Otherwise, let it die of RequestTimeout
     * starvation.
     */
    case logSubrange @ LogSubrange(rangeStart, rangeEnd, events) if rangeEnd >= nextSeq =>
      val oldNextSeq = nextSeq
      processLogSubrange(logSubrange)
      if (oldNextSeq != nextSeq)
        siriusSupActor ! DecisionHint(nextSeq - 1)

      if (chunkSize == (rangeEnd - rangeStart) + 1) {
        requestNextChunk()
      } else if (startupCatchupDuration == None) {
        startupCatchupDuration = Some(System.currentTimeMillis() - startupTimestamp)
      }

    case RequestGaps =>
      runGapFetcher()

    case Terminated(terminatedActor) =>
      gapFetcher match {
        case Some(gapActor) if gapActor == terminatedActor =>
          gapFetcher = None
        case _ => // not worried about any other actors, or if old fetchers are reporting dead
      }

  }

  def processLogSubrange(logSubrange: LogSubrange) {
    logSubrange match {
      case LogSubrange(rangeStart, rangeEnd, events) =>
        // apply all events in range s.t. event.sequence >= nextSeq
        events.foreach((event: OrderedEvent) => {
          if (event.sequence >= nextSeq) {
            stateSupActor ! event
            traceLogger.debug("Wrote caught-up event for sequence={}, " +
              "time since original command submission: {}ms", event.sequence,
              System.currentTimeMillis() - event.timestamp)
          }
        })
        nextSeq = rangeEnd + 1
        // drop things in the event buffer < nextSeq
        eventBuffer.dropWhile((slot, _) => slot < nextSeq)
      case _ =>
    }
  }

  /**
   * Add this event to the event buffer and fire off the method that flushes
   * any ready decisions from the buffer.
   *
   * This method will only have side-effects if the event *should* be processed.
   * That is, if the sequence number of the event is >= nextSeq.
   *
   * @param event OrderedEvent to add/process
   */
  private def processOrderedEvent(event: OrderedEvent) {
    if (event.sequence >= nextSeq) {
      eventBuffer.put(event.sequence, event)
      executeReadyDecisions()
    }
  }

  /**
   * With side effects, applies all decisions in the local queue ready for application.  The
   * side effects are that the queue is trimmed and nextSeq is updated.
   */
  private def executeReadyDecisions() {
    val oldNextSeq = nextSeq

    @tailrec
    def applyNextReadyDecision() {
      if (!eventBuffer.isEmpty && eventBuffer.containsKey(nextSeq)) {
        // remove next event and send to stateSupActor
        stateSupActor ! eventBuffer.remove(nextSeq)
        nextSeq += 1
        applyNextReadyDecision()
      } else {
        // terminate
      }
    }
    applyNextReadyDecision()
    if (oldNextSeq != nextSeq) siriusSupActor ! DecisionHint(nextSeq - 1)
  }

  /**
   * Called when the last chunk we received was helpful.  Request another one if
   * the fetcher is alive and well.
   */
  private[bridge] def requestNextChunk() {
    gapFetcher match {
      case Some(fetcher) =>
        fetcher ! RequestFromSeq(nextSeq)
      case _ =>
      // do nothing for now; only handle the case where the fetcher is alive and well
    }
  }

  /**
   * Checks whether there is currently a GapFetcher running.  If there is,
   * does nothing, assuming the fetcher is still working properly.  Otherwise,
   * starts a new one, which will start requesting gaps at nextSeq (next
   * sequence number needed to enable writes from the buffer).
   */
  private[bridge] def runGapFetcher() {
    gapFetcher match {
      case None =>
        gapFetcher = createGapFetcher(nextSeq)
      case _ =>
        // do nothing, fetcher is still working
    }
  }

  private[bridge] def createGapFetcher(seq: Long): Option[ActorRef] = {
    val randomMember = membershipHelper.getRandomMember
    randomMember match {
      case Some(member) =>
        traceLogger.debug("Creating gap fetcher to request {} events starting at {} from {}", chunkSize, seq, member)
        val gapFetcher = childProvider.createGapFetcher(seq, member, self)
        context.watch(gapFetcher)
        Some(gapFetcher)
      case None =>
        logger.warning("Failed to create GapFetcher: could not get remote" +
          "member for transfer request.")
        None
    }
  }

  /**
   * Monitoring hooks
   */
  trait PaxosStateBridgeInfoMBean {
    def getNextSeq: Long
    def getEventBufferSize: Int
    def getStartupCatchupDuration: Option[Long]
  }

  class PaxosStateBridgeInfo extends PaxosStateBridgeInfoMBean {
    def getNextSeq = nextSeq
    def getEventBufferSize = eventBuffer.size
    def getStartupCatchupDuration = startupCatchupDuration
  }
}
