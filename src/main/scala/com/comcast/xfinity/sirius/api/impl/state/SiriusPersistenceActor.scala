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
package com.comcast.xfinity.sirius.api.impl.state

import akka.actor.{Props, Actor, ActorRef}
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.{SiriusConfiguration, SiriusResult}
import com.comcast.xfinity.sirius.api.impl._
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import scala.math.min

object SiriusPersistenceActor {

  /**
   * Trait encapsulating _queries_ into Sirius's log's state,
   * that is, the state of persisted data
   */
  sealed trait LogQuery

  case object GetLogSize extends LogQuery
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
  case class GetLogSubrange(begin: Long, end: Long) extends LogQuery

  /**
   * Message encapsulating a range of events, as asked for by
   * a GetSubrange message.
   *
   * This message is necessary due to type erasure, OrderedEvent
   * is erased from the resultant List
   *
   * @param lowestSeqContained lowest seq contained in this range.  may or may not
   *                           have a corresponding OrderedEvent in events (could
   *                           have been compacted away)
   * @param highestSeqContained highest seq contained in this range.  may or may not
   *                           have a corresponding OrderedEvent in events (could
   *                           have been compacted away)
   * @param events the OrderedEvents in this range, in order
   */
  case class LogSubrange(lowestSeqContained: Long,
                         highestSeqContained: Long,
                         events: List[OrderedEvent])

  trait NewLogSubrange
  trait PopulatedSubrange extends NewLogSubrange {
    def rangeStart: Long
    def rangeEnd: Long
    def events: List[OrderedEvent]
  }
  case object EmptySubrange extends NewLogSubrange
  case class PartialSubrange(rangeStart: Long, rangeEnd: Long, events: List[OrderedEvent]) extends PopulatedSubrange
  case class CompleteSubrange(rangeStart: Long, rangeEnd: Long, events: List[OrderedEvent]) extends PopulatedSubrange

  /**
   * Message requesting maximum sequence number from a
   * SiriusPersistenceActor.  The actor is expected to reply with
   * the Long value of the next sequence number.
   */
  case object GetNextLogSeq extends LogQuery

  /**
   * Create Props for a SiriusPersistenceActor.
   *
   * @param stateActor ActorRef of state supervisor
   * @param siriusLog active SiriusLog
   * @param config SiriusConfiguration object full of all kinds of configuration goodies, see SiriusConfiguration for
   *               more information
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(stateActor: ActorRef, siriusLog: SiriusLog, config: SiriusConfiguration): Props = {
    Props(classOf[SiriusPersistenceActor], stateActor, siriusLog, config)
  }
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
 */
class SiriusPersistenceActor(stateActor: ActorRef,
                             siriusLog: SiriusLog,
                             config: SiriusConfiguration) extends Actor with MonitoringHooks {

  import SiriusPersistenceActor._

  var numWrites = 0L
  var cummWeightedAvg = 0L
  var lastWriteTime = 0L

  override def preStart() {
    registerMonitor(new SiriusPersistenceActorInfo, config)
  }

  override def postStop() {
    unregisterMonitors(config)
  }

  //XXX: this is a rough cummulative linear weighted avg.  Might want to see what else is out there in future
  /*
  Linear Weighted Cumulative Moving Average
        http://www.had2know.com/finance/cumulative-weighted-moving-average-calculator.html
        L(1) = x(1)
        L(i+1) = (2/(i+2))x(i+1) + (i/(i+2))L(i)
  */
  def weightedAvg(num: Long, curr: Long, cummulative: Long): Long = num match {
    case (n:Long) if n > 1 =>
      val rhs = (2.0/(n+1))*curr
      val lhs = (n.toDouble-1)/(n.toDouble+1)*cummulative
      (rhs+lhs).toLong
    case _ => curr
   }

  def receive = {
    case event: OrderedEvent =>
      val now = System.currentTimeMillis()
      siriusLog.writeEntry(event)
      stateActor ! event.request

      val thisWriteTime = System.currentTimeMillis() - now
      numWrites += 1
      cummWeightedAvg = weightedAvg(numWrites,thisWriteTime,cummWeightedAvg)

      lastWriteTime = thisWriteTime

    case GetLogSubrange(rangeStart, rangeEnd) if rangeEnd < siriusLog.getNextSeq => // we can answer fully
      val events = siriusLog.foldLeftRange(rangeStart, rangeEnd)(List[OrderedEvent]())(
        (acc, event) => event :: acc
      ).reverse
      sender ! CompleteSubrange(rangeStart, rangeEnd, events)

    case GetLogSubrange(rangeStart, rangeEnd) if siriusLog.getNextSeq <= rangeStart => // we can't send anything useful back
      sender ! EmptySubrange

    case GetLogSubrange(rangeStart, rangeEnd) => // we can respond partially
      val lastSeq = siriusLog.getNextSeq - 1
      val events = siriusLog.foldLeftRange(rangeStart, lastSeq)(List[OrderedEvent]())(
        (acc, event) => event :: acc
      ).reverse
      sender ! PartialSubrange(rangeStart, lastSeq, events)

    case GetNextLogSeq =>
      sender ! siriusLog.getNextSeq

    case GetLogSize =>
      sender ! siriusLog.size

    // XXX SiriusStateActor responds to writes with a SiriusResult, we don't really want this
    // anymore, and it should be eliminated in a future commit
    case _: SiriusResult =>
  }

  /**
   * Monitoring hooks
   */
  trait SiriusPersistenceActorInfoMBean {
    def getAveragePersistDuration: Double
    def getSiriusLogSize: Long
  }

  class SiriusPersistenceActorInfo extends SiriusPersistenceActorInfoMBean {

    def getAveragePersistDuration = cummWeightedAvg
    def getSiriusLogSize = siriusLog.size
  }
}
