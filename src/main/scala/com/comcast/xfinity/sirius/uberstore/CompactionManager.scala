package com.comcast.xfinity.sirius.uberstore

import akka.actor.{ActorContext, ActorRef, Props, Actor}
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import akka.event.Logging
import akka.util.duration._
import com.comcast.xfinity.sirius.uberstore.CompactionActor.CompactionComplete
import com.comcast.xfinity.sirius.admin.MonitoringHooks
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.uberstore.CompactionManager.ChildProvider

object CompactionManager {
  sealed trait CompactionMessage
  case object Compact extends CompactionMessage
  case object GetState extends CompactionMessage

  trait CompactionManagerInfoMBean {
    def getLastCompactionStarted: Option[Long]
    def getLastCompactionDuration: Option[Long]
    def getCompactionActor: Option[ActorRef]
  }

  /**
   * Factory for creating children actors of CompactionManager.
   *
   * @param siriusLog active SiriusLog for compaction
   */
  class ChildProvider(siriusLog: SiriusLog) {
    def createCompactionActor()(implicit context: ActorContext): ActorRef =
      context.actorOf(CompactionActor.props(siriusLog), "compaction")
  }

  /**
   * Create Props for CompactionManager, actor responsible for managing compaction processes.
   * It spins up CompactionActors, handles state requests, and keeps tabs on compaction duration.
   *
   * @param siriusLog active SiriusLog for compaction
   * @param config SiriusConfiguration configuration properties container
   */
  def props(siriusLog: SiriusLog)(implicit config: SiriusConfiguration): Props = {
    //Props(classOf[CompactionManager], new ChildProvider(siriusLog), config)
    Props(new CompactionManager(new ChildProvider(siriusLog), config))
  }
}

class CompactionManager(childProvider: ChildProvider,
                        config: SiriusConfiguration)
      extends Actor with MonitoringHooks {
  import CompactionManager._

  val compactionScheduleMins = config.getProp(SiriusConfiguration.COMPACTION_SCHEDULE_MINS, 0) // off by default
  val compactionCancellable = compactionScheduleMins match {
    case interval if interval > 0 =>
      Some(context.system.scheduler.schedule(interval minutes, interval minutes, self, Compact))
    case _ => None
  }

  val logger = Logging(context.system, "Sirius")

  var lastCompactionStarted: Option[Long] = None
  var lastCompactionDuration: Option[Long] = None
  var compactionActor: Option[ActorRef] = None

  def startCompaction: ActorRef = {
    val actor = childProvider.createCompactionActor
    actor ! Compact
    actor
  }

  override def preStart() {
    registerMonitor(new CompactionManagerInfo, config)
  }

  override def postStop() {
    unregisterMonitors(config)
    compactionCancellable match {
      case Some(cancellable) => cancellable.cancel()
      case None =>
    }
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
          // do nothing; actor is already compacting.
      }
  }

  class CompactionManagerInfo extends CompactionManagerInfoMBean {
    def getLastCompactionStarted = lastCompactionStarted
    def getLastCompactionDuration = lastCompactionDuration
    def getCompactionActor = compactionActor
  }

}
