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

import akka.actor._
import com.comcast.xfinity.sirius.api.{SiriusConfiguration, SiriusResult}
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import com.comcast.xfinity.sirius.util.RichJTreeMap
import com.comcast.xfinity.sirius.api.impl.bridge.PaxosStateBridge.ChildProvider
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor._
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.PartialSubrange
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.DecisionHint
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Decision
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.CompleteSubrange
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Command
import scala.language.postfixOps
import scala.concurrent.duration._

object PaxosStateBridge {

  /**
   * Factory for creating the children actors of PaxosStateBridge.
   *
   * @param config the SiriusConfiguration for this node
   */
  class ChildProvider(config: SiriusConfiguration, membershipHelper: MembershipHelper) {
    def createCatchupSupervisor()(implicit context: ActorContext) = {
      context.actorOf(CatchupSupervisor.props(membershipHelper, config))
    }
  }

  /**
   * Create Props for a PaxosStateBridge actor.
   *
   * @param startingSeq the sequence number to start with
   * @param stateSupervisor reference to the subsystem encapsulating system state.
   * @param siriusSupervisor reference to the Sirius Supervisor Actor for routing
   *                         DecisionHints to the Paxos Subsystem
   * @param membershipHelper reference to object that knows how to get a random
   *                         remote cluster member
   * @param config SiriusConfiguration for this node
   * @return Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(startingSeq: Long,
            stateSupervisor: ActorRef,
            siriusSupervisor: ActorRef,
            membershipHelper: MembershipHelper,
            config: SiriusConfiguration): Props = {
    val childProvider = new ChildProvider(config, membershipHelper)
    val catchupFreq = config.getProp(SiriusConfiguration.LOG_REQUEST_FREQ_SECS, 30).seconds

    Props(classOf[PaxosStateBridge], startingSeq, stateSupervisor, siriusSupervisor, childProvider, catchupFreq, config)
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
 * @param stateSupervisor reference to the subsystem encapsulating system state.
 *            In the future as the code settles down we may want to have this
 *            directly point to the persistence layer, but for now we funnel
 *            everything through the state subsystem supervisor for abstraction,
 *            such that we can easily refactor and not worry about messing stuff
 *            up.
 * @param siriusSupervisor reference to the Sirius Supervisor Actor for routing
 *          DecisionHints to the Paxos Subsystem
 */
class PaxosStateBridge(startingSeq: Long,
                       stateSupervisor: ActorRef,
                       siriusSupervisor: ActorRef,
                       childProvider: ChildProvider,
                       catchupFreq: FiniteDuration,
                       config: SiriusConfiguration)
      extends Actor with MonitoringHooks {

  implicit val executionContext = context.system.dispatcher

  var nextSeq: Long = startingSeq
  var eventBuffer = RichJTreeMap[Long, OrderedEvent]()

  val catchupSupervisor = childProvider.createCatchupSupervisor()
  val catchupSchedule = context.system.scheduler.schedule(catchupFreq, catchupFreq, self, InitiateCatchup)

  // monitor stats, for original catchup duration
  var startupCatchupDuration: Option[Long] = None
  val startupTimestamp = System.currentTimeMillis()

  override def preStart() {
    registerMonitor(new PaxosStateBridgeInfo, config)
  }
  override def postStop() {
    catchupSchedule.cancel()
    unregisterMonitors(config)
  }

  def receive = {

    case Decision(seq, Command(client, ts, op)) if seq >= nextSeq && !eventBuffer.containsKey(seq) =>
      eventBuffer.put(seq, OrderedEvent(seq, ts, op))
      while (eventBuffer.containsKey(nextSeq)) {
        stateSupervisor ! eventBuffer.remove(nextSeq)
        nextSeq += 1
      }
      client ! SiriusResult.none()
      siriusSupervisor ! DecisionHint(nextSeq - 1)

    case InitiateCatchup =>
      catchupSupervisor ! InitiateCatchup(nextSeq)

    case subrange: CompleteSubrange =>
      applySubrange(subrange)
      catchupSupervisor ! ContinueCatchup(nextSeq)

    case subrange: PartialSubrange =>
      applySubrange(subrange)
      updateCatchupDuration()
      catchupSupervisor ! StopCatchup

    case EmptySubrange =>
      updateCatchupDuration()
      catchupSupervisor ! StopCatchup

  }

  private def updateCatchupDuration() {
    if (startupCatchupDuration.isEmpty) { // some accounting: speed of our first catchup
      startupCatchupDuration = Some(System.currentTimeMillis() - startupTimestamp)
    }
  }

  private def applySubrange(subrange: PopulatedSubrange) {
    // for each useful event, send it to the stateSupervisor
    subrange.events.dropWhile(_.sequence < nextSeq).foreach {
      case event =>
        stateSupervisor ! event
    }
    // update nextSeq based on the declared rangeEnd, no matter what events were actually included
    if (subrange.rangeEnd >= nextSeq) {
      nextSeq = subrange.rangeEnd + 1
    }
    // dump out of the buffer events that no longer matter
    eventBuffer.dropWhile((slot, _) => slot < nextSeq)
    // let parent know about the new nextSeq
    siriusSupervisor ! DecisionHint(nextSeq - 1)
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
