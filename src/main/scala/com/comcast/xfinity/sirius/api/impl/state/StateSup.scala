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
   * Factory for creating children of StateSup.
   *
   * @param requestHandler the RequestHandler to apply updates to and perform Gets on
   * @param siriusLog the SiriusLog to persist OrderedEvents to
   * @param config SiriusConfiguration object for configuring children actors.
   */
  private[state] class ChildProvider(requestHandler: RequestHandler, siriusLog: SiriusLog, config: SiriusConfiguration) {
    def createStateActor()(implicit context: ActorContext): ActorRef =
      context.actorOf(SiriusStateActor.props(requestHandler), "state")

    def createPersistenceActor(stateActor: ActorRef)(implicit context: ActorContext): ActorRef =
      context.actorOf(SiriusPersistenceActor.props(stateActor, siriusLog, config), "persistence")
  }

  /**
   * Create a StateSup managing the state of requestHandler and persisting data to siriusLog.
   *
   * @param requestHandler the RequestHandler containing the callbacks for manipulating this instance's state
   * @param siriusLog the log to be used for persisting events
   * @param siriusStateAgent agent containing information on the state of the system
   * @param config SiriusConfiguration object full of all kinds of configuration goodies, see SiriusConfiguration for
   *               more information
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(requestHandler: RequestHandler,
            siriusLog: SiriusLog,
            siriusStateAgent: Agent[SiriusState],
            config: SiriusConfiguration): Props = {
    val childProvider = new ChildProvider(requestHandler, siriusLog, config)
    Props(classOf[StateSup], requestHandler, siriusLog, siriusStateAgent, childProvider, config)
  }
}

/**
 * Actor for supervising state related matters (in memory state and persistent state).
 *
 * @param requestHandler the RequestHandler to apply updates to and perform Gets on
 * @param siriusLog the SiriusLog to persist OrderedEvents to
 * @param siriusStateAgent agent containing information on the state of the system.
 * @param childProvider factory for creating children of StateSup
 * @param config SiriusCOnfiguration object for configuring children actors.
 */
// TODO rename this StateSupervisor
class StateSup(requestHandler: RequestHandler,
               siriusLog: SiriusLog,
               siriusStateAgent: Agent[SiriusState],
               childProvider: StateSup.ChildProvider,
               config: SiriusConfiguration)
    extends Actor with MonitoringHooks {

  val logger = Logging(context.system, "Sirius")

  val stateActor = childProvider.createStateActor
  val persistenceActor = childProvider.createPersistenceActor(stateActor)

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

  // TODO perhaps this should be pulled out into a BootstrapActor. The StateSup should really only supervise.
  private def bootstrapState() {
    val start = System.currentTimeMillis

    logger.info("Beginning SiriusLog replay at {}", start)
    // TODO convert this to foreach
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
