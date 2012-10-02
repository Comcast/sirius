package com.comcast.xfinity.sirius.api.impl.bridge

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.{SiriusConfiguration, SiriusResult}
import annotation.tailrec
import akka.actor.{Props, Actor, ActorRef}
import akka.util.duration._
import akka.event.Logging
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.LogSubrange
import com.comcast.xfinity.sirius.util.RichJTreeMap

object PaxosStateBridge {
  case object RequestGaps
  case class RequestFromSeq(begin: Long)

  def apply(startingSeq: Long,
            stateSupActor: ActorRef,
            siriusSupActor: ActorRef,
            membershipHelper: MembershipHelper,
            config: SiriusConfiguration) = {

    // TODO add grabbing request gaps delay from config, pass into constructor?
    new PaxosStateBridge(startingSeq, stateSupActor, siriusSupActor, membershipHelper)(config)
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
                       membershipHelper: MembershipHelper)
                      (implicit config: SiriusConfiguration = new SiriusConfiguration)
      extends Actor with MonitoringHooks {
    import PaxosStateBridge._

  // for gapFetcher instantiation
  val chunkSize = config.getProp(SiriusConfiguration.LOG_REQUEST_CHUNK_SIZE, 1000)
  val chunkReceiveTimeout = config.getProp(SiriusConfiguration.LOG_REQUEST_RECEIVE_TIMEOUT_SECS, 5)

  val logger = Logging(context.system, "Sirius")
  val traceLogger = Logging(context.system, "SiriusTrace")

  var gapFetcher: Option[ActorRef] = None

  // TODO: make this configurable- it's cool hardcoded for now, but once
  //       SiriusConfig matures this would be pretty clean to configure
  // also, start immediately.
  val requestGapsCancellable =
    context.system.scheduler.schedule(30 seconds, 30 seconds, self, RequestGaps)

  var nextSeq: Long = startingSeq
  var eventBuffer = new RichJTreeMap[Long, OrderedEvent]()

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
     * Decision.command.client is sent a SiriusResult.none message to aknowledge the
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

      if (chunkSize == (rangeEnd - rangeStart) + 1)
        requestNextChunk()

    case RequestGaps =>
      //XXX: logging unreadyDecisions... should remove in favor or better monitoring later
      logger.debug("Unready Decisions count: {}", eventBuffer.size)
      runGapFetcher()
  }

  def processLogSubrange(logSubrange: LogSubrange) {
    logSubrange match {
      case LogSubrange(rangeStart, rangeEnd, events) =>
        // apply all events in range s.t. event.seqence >= nextSeq
        events.foreach((event: OrderedEvent) => {
          if (event.sequence >= nextSeq) {
            stateSupActor ! event
          }
          traceLogger.debug("Writing caught-up event for sequence={}, " +
            "time since original command submission: {}ms", event.sequence,
            System.currentTimeMillis() - event.timestamp)
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
      case Some(fetcher) if !fetcher.isTerminated =>
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
      case Some(fetcher) if !fetcher.isTerminated =>
        // do nothing, fetcher is still working
      case _ =>
        gapFetcher = createGapFetcher(nextSeq)
    }
  }

  private[bridge] def createGapFetcher(seq: Long): Option[ActorRef] = {
    val randomMember = membershipHelper.getRandomMember
    randomMember match {
      case Some(member) =>
        Some(createGapFetcherActor(seq, member))
      case None =>
        logger.warning("Failed to create GapFetcher: could not get remote" +
          "member for transfer request.")
        None
    }
  }

  def createGapFetcherActor(seq: Long, target: ActorRef) =
    context.actorOf(Props(new GapFetcher(seq, target, self, chunkSize, chunkReceiveTimeout)), "gapfetcher")

  /**
   * Monitoring hooks
   */
  trait PaxosStateBridgeInfoMBean {
    def getNextSeq: Long
    def getEventBufferSize: Int
  }

  class PaxosStateBridgeInfo extends PaxosStateBridgeInfoMBean {
    def getNextSeq = nextSeq
    def getEventBufferSize = eventBuffer.size
  }
}
