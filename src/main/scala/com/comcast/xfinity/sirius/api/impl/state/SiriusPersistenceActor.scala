package com.comcast.xfinity.sirius.api.impl.state

import akka.actor.Actor
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.{SiriusConfiguration, SiriusResult}
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl._
import persistence.BoundedLogRange
import com.comcast.xfinity.sirius.admin.MonitoringHooks

object SiriusPersistenceActor {

  /**
   * Trait encapsulating _queries_ into Sirius's log's state,
   * that is the state of persisted data.g
   */
  sealed trait LogQuery

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
   * @param events the OrderedEvents in this range, in order
   */
  case class LogSubrange(events: List[OrderedEvent])

  /**
   * Message requesting maximum sequence number from a
   * SiriusPersistenceActor.  The actor is expected to reply with
   * the Long value of the next sequence number.
   */
  case object GetNextLogSeq extends LogQuery
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
    (implicit config: SiriusConfiguration = new SiriusConfiguration) extends Actor with MonitoringHooks {

  import SiriusPersistenceActor._

  var numWrites = 0L
  var totalWriteTime = 0L

  override def preStart() {
    // if replay is done externally, do we still need this?  my thought is yes,
    //  because if we got to this point it _implies_ that we have completed replay,
    //  but we can probably move this elsewhere where it's actually right after
    //  replay
    siriusStateAgent send (_.copy(persistenceInitialized = true))
    registerMonitor(new SiriusPersistenceActorInfo, config)
  }

  override def postStop() {
    unregisterMonitors(config)
  }

  def receive = {
    case event: OrderedEvent =>
      val now = System.currentTimeMillis()
      siriusLog.writeEntry(event)
      stateActor ! event.request

      numWrites += 1
      totalWriteTime += System.currentTimeMillis() - now

    // XXX: cap max request chunk size hard coded at 1000 for now for sanity
    case GetLogSubrange(begin, end) if begin <= end && (begin - end) < 1000 =>
      val chunkRange = siriusLog.createIterator(BoundedLogRange(begin, end))
      sender ! LogSubrange(chunkRange.toList)

    case GetNextLogSeq =>
      sender ! siriusLog.getNextSeq

    // SiriusStateActor responds to writes with a SiriusResult, we don't really want this
    // anymore, and it should be eliminated in a future commit
    case _: SiriusResult =>
  }

  /**
   * Monitoring hooks
   */
  trait SiriusPersistenceActorInfoMBean {
    def getAveragePersistDuration: Double
  }

  class SiriusPersistenceActorInfo extends SiriusPersistenceActorInfoMBean {
    def getAveragePersistDuration = numWrites match {
      case 0 => 0
      case _ => totalWriteTime / numWrites
    }
  }
}