package com.comcast.xfinity.sirius.api.impl.state

import com.comcast.xfinity.sirius.api.{SiriusConfiguration, RequestHandler}
import akka.agent.Agent
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.impl._
import akka.event.Logging
import state.SiriusPersistenceActor.LogQuery
import akka.actor.{ActorContext, Props, ActorRef, Actor}
import com.comcast.xfinity.sirius.admin.MonitoringHooks

object StateSup {

  /**
   * Helper class for creating Actors within the StateSup,
   * externalized so we can mock it out and test.
   */
  private[state] class ChildProvider {
    def makeStateActor(requestHandler: RequestHandler)
                      (implicit context: ActorContext): ActorRef =
      context.actorOf(Props(new SiriusStateActor(requestHandler)), "state")

    def makePersistenceActor(stateActor: ActorRef,
                             siriusLog: SiriusLog,
                             config: SiriusConfiguration)
                            (implicit context: ActorContext): ActorRef =
      context.actorOf(Props(new SiriusPersistenceActor(stateActor, siriusLog)(config)), "persistence")
  }

  /**
   * Create a StateSup managing the state of requestHandler and persisting data to siriusLog.
   * As this instantiates an actor, it must be called via the Props factory object using actorOf.
   *
   * @param requestHandler the RequestHandler to apply updates to and perform Gets on
   * @param siriusLog the SiriusLog to persist OrderedEvents to
   * @param siriusStateAgent agent containing information on the state of the system.
   *            This should eventually go bye bye
   *
   * @returns the StateSup managing the state subsystem.
   */
  def apply(requestHandler: RequestHandler,
            siriusLog: SiriusLog,
            siriusStateAgent: Agent[SiriusState],
            config: SiriusConfiguration): StateSup = {
    val childProvider = new ChildProvider
    new StateSup(requestHandler, siriusLog, siriusStateAgent, childProvider)(config)
  }
}

/**
 * Actors for supervising state related matters
 */
class StateSup(requestHandler: RequestHandler,
               siriusLog: SiriusLog,
               siriusStateAgent: Agent[SiriusState],
               childProvider: StateSup.ChildProvider)
              (implicit config: SiriusConfiguration = new SiriusConfiguration)
    extends Actor with MonitoringHooks {

  val logger = Logging(context.system, "Sirius")

  val stateActor = childProvider.makeStateActor(requestHandler)
  val persistenceActor = childProvider.makePersistenceActor(stateActor, siriusLog, config)

  // monitor stuff
  var eventReplayFailureCount: Long = 0
  // it would be cool to be able to observe this during boot...
  var bootstrapTime: Option[Long] = None

  override def preStart() {
    registerMonitor(new StateInfo, config)
    bootstrapState()
    siriusStateAgent send (_.copy(stateInitialized = true))
  }

  override def postStop() {
    unregisterMonitors(config)
  }

  def receive = {
    case get: Get =>
      stateActor forward get

    case orderedEvent: OrderedEvent =>
      persistenceActor ! orderedEvent

    case logQuery: LogQuery =>
      persistenceActor forward logQuery

  }

  private def bootstrapState() {
    val start = System.currentTimeMillis

    logger.info("Beginning SiriusLog replay at {}", start)
    // perhaps the foreach abstraction will be nice to have back
    //  in SiriusLog?
    siriusLog.foldLeft(())(
      (_, orderedEvent) =>
        try {
          orderedEvent.request match {
            case Put(key, body) => requestHandler.handlePut(key, body)
            case Delete(key) => requestHandler.handleDelete(key)
          }
        } catch {
          case rte: RuntimeException =>
            eventReplayFailureCount += 1
            logger.error("Exception replaying {}: {}", orderedEvent, rte)
        }
    )
    val totalBootstrapTime = System.currentTimeMillis - start
    bootstrapTime = Some(totalBootstrapTime)
    logger.info("Replayed SiriusLog in {}ms", totalBootstrapTime)
  }

  trait StateInfoMBean {
    def getEventReplayFailureCount: Long
    def getBootstrapTime: String
  }

  class StateInfo extends StateInfoMBean {
    def getEventReplayFailureCount = eventReplayFailureCount
    def getBootstrapTime = bootstrapTime.toString
  }
}