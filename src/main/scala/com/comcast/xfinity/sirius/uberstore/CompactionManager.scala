package com.comcast.xfinity.sirius.uberstore

import akka.actor.{ActorRef, Props, Actor}
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import akka.event.Logging
import com.comcast.xfinity.sirius.uberstore.CompactionActor.CompactionComplete
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.uberstore.UberStore.CompactionState

object CompactionManager {
  sealed trait CompactionMessage
  case object Compact extends CompactionMessage
  case object GetState extends CompactionMessage

  trait CompactionSchedulingActorInfoMBean {
    def getLastCompactionStarted: Option[Long]
    def getLastCompactionDuration: Option[Long]
    def getCompactionState: CompactionState
    def getCompactionActor: Option[ActorRef]
  }
}

/**
 * Actor responsible for managing compaction processes. It spins up CompactionActors, handles
 * state requests, and keeps tabs on compaction duration.
 *
 * @param siriusLog active SiriusLog for compaction
 * @param config SiriusConfiguration configuration properties container
 */
class CompactionManager(siriusLog: SiriusLog)
                       (implicit config: SiriusConfiguration = new SiriusConfiguration)
      extends Actor with MonitoringHooks {
  import CompactionManager._

  val logger = Logging(context.system, "Sirius")

  var lastCompactionStarted: Option[Long] = None
  var lastCompactionDuration: Option[Long] = None
  var compactionActor: Option[ActorRef] = None

  def createCompactionActor: ActorRef =
    context.actorOf(Props(new CompactionActor(siriusLog)))

  def startCompaction: ActorRef = {
    val actor = createCompactionActor
    actor ! Compact
    actor
  }

  override def preStart() {
    registerMonitor(new CompactionSchedulingActorInfo, config)
  }

  def receive = {
    case CompactionComplete =>
      lastCompactionDuration = lastCompactionStarted match {
        case Some(startTime) => Some(System.currentTimeMillis() - startTime)
        case None => None // really shouldn't happen, but y'know
      }

      compactionActor = None

    case Compact =>
      compactionActor match {
        case None =>
          compactionActor = Some(startCompaction)
          lastCompactionStarted = Some(System.currentTimeMillis())

        case Some(actor) if actor.isTerminated =>
          logger.warning("Restarting compaction, found terminated compaction actor: {}", actor)
          compactionActor = Some(startCompaction)
          lastCompactionStarted = Some(System.currentTimeMillis())

        case Some(actor) =>
          // do nothing; actor is already compacting. XXX maybe log too-small compaction window or stuck compaction?

      }

    case GetState =>
      sender ! siriusLog.getCompactionState
  }

  class CompactionSchedulingActorInfo extends CompactionSchedulingActorInfoMBean {
    def getLastCompactionStarted = lastCompactionStarted
    def getLastCompactionDuration = lastCompactionDuration
    def getCompactionState = siriusLog.getCompactionState
    def getCompactionActor = compactionActor
  }

}
