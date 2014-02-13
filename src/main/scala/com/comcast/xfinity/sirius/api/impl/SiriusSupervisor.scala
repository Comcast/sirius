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
package com.comcast.xfinity.sirius.api.impl

import bridge.PaxosStateBridge
import com.comcast.xfinity.sirius.api.impl.membership.{MembershipHelper, MembershipActor}
import paxos.PaxosMessages.PaxosMessage
import akka.actor._
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.paxos.Replica
import scala.concurrent.duration._
import paxos.PaxosSup
import state.SiriusPersistenceActor.LogQuery
import state.StateSup
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import akka.event.Logging
import com.comcast.xfinity.sirius.api.{SiriusConfiguration, RequestHandler}
import status.StatusWorker
import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver
import status.StatusWorker.StatusQuery
import com.comcast.xfinity.sirius.uberstore.CompactionManager
import com.comcast.xfinity.sirius.uberstore.CompactionManager.CompactionMessage
import com.comcast.xfinity.sirius.api.impl.SiriusSupervisor.{ChildProvider, CheckPaxosMembership}
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor.MembershipMessage
import scala.language.postfixOps

object SiriusSupervisor {

  sealed trait SupervisorMessage
  case object IsInitializedRequest extends SupervisorMessage
  case object CheckPaxosMembership extends SupervisorMessage

  case class IsInitializedResponse(initialized: Boolean)

  /**
   * Factory for creating the children actors of SiriusSupervisor.
   *
   * @param requestHandler User implemented RequestHandler.
   * @param siriusLog Interface into the Sirius persistent log.
   * @param config the SiriusConfiguration for this node
   */
  protected[impl] class ChildProvider(requestHandler: RequestHandler, siriusLog: SiriusLog, config: SiriusConfiguration) {

    def createStateAgent()(implicit context: ActorContext): Agent[SiriusState] =
      Agent(new SiriusState)(context.dispatcher)

    def createMembershipAgent()(implicit context: ActorContext) =
      Agent(Map[String, Option[ActorRef]]())(context.dispatcher)

    def createStateSupervisor(stateAgent: Agent[SiriusState])(implicit context: ActorContext) =
      context.actorOf(StateSup.props(requestHandler, siriusLog, stateAgent, config), "state")

    def createMembershipActor(membershipAgent: Agent[Map[String, Option[ActorRef]]])(implicit context: ActorContext) =
      context.actorOf(MembershipActor.props(membershipAgent, config), "membership")

    def createStateBridge(stateSupervisor: ActorRef, siriusSupervisor: ActorRef, membershipHelper: MembershipHelper)
                         (implicit context: ActorContext) = {
      context.actorOf(PaxosStateBridge
        .props(siriusLog.getNextSeq, stateSupervisor, siriusSupervisor, membershipHelper, config),
        "paxos-state-bridge")
    }

    def createPaxosSupervisor(membership: MembershipHelper, performFun: Replica.PerformFun)(implicit context: ActorContext) =
      context.actorOf(PaxosSup.props(membership, siriusLog.getNextSeq, performFun, config), "paxos")

    def createStatusSubsystem(siriusSupervisor: ActorRef)(implicit context: ActorContext) = {
      val supervisorAddress = AkkaExternalAddressResolver(context.system).externalAddressFor(siriusSupervisor)
      context.actorOf(StatusWorker.props(supervisorAddress, config), "status")
    }

    def createCompactionManager()(implicit context: ActorContext) =
      context.actorOf(CompactionManager.props(siriusLog)(config), "compactionManager")
  }

  /**
   * Create Props for a SiriusSupervisor actor.
   *
   * @param requestHandler the RequestHandler containing the callbacks for manipulating this instance's state
   * @param siriusLog the log to be used for persisting events
   * @param config SiriusConfiguration object full of all kinds of configuration goodies, see SiriusConfiguration for
   *               more information
   * @return  Props for creating this actor, which can then be further configured
   *         (e.g. calling `.withDispatcher()` on it)
   */
  def props(requestHandler: RequestHandler,
              siriusLog: SiriusLog,
              config: SiriusConfiguration): Props = {
    Props(classOf[SiriusSupervisor], new ChildProvider(requestHandler, siriusLog, config), config)
  }
}

/**
 * SiriusSupervisor is responsible for managing the top level of the actor hierarchy. It creates the actors
 * and manages their lifecycles, handles system initialization, and routes incoming messages. Messages from
 * remote systems come here first.
 *
 * @param childProvider factory for creating children actors
 * @param config SiriusConfiguration object containing node configuration
 */
private[impl] class SiriusSupervisor(childProvider: ChildProvider, config: SiriusConfiguration) extends Actor {

  implicit val executionContext = context.system.dispatcher

  val siriusStateAgent = childProvider.createStateAgent()
  val membershipAgent = childProvider.createMembershipAgent()
  val membershipHelper = MembershipHelper(membershipAgent, context.self)
  val stateSup = childProvider.createStateSupervisor(siriusStateAgent)
  val membershipActor = childProvider.createMembershipActor(membershipAgent)
  var orderingActor: Option[ActorRef] = None
  var compactionManager : Option[ActorRef] = None
  val statusSubsystem = childProvider.createStatusSubsystem(self)
  val stateBridge = childProvider.createStateBridge(stateSup, context.self, membershipHelper)

  private val logger = Logging(context.system, "Sirius")

  val initSchedule = context.system.scheduler
    .schedule(0 seconds, 50 milliseconds, self, SiriusSupervisor.IsInitializedRequest)

  val checkIntervalSecs = config.getProp(SiriusConfiguration.PAXOS_MEMBERSHIP_CHECK_INTERVAL, 2.0)
  val membershipCheckSchedule = context.system.scheduler.
    schedule(0 seconds, checkIntervalSecs seconds, self, CheckPaxosMembership)

  override def postStop() {
    membershipCheckSchedule.cancel()
  }

  def receive = {
    // TODO simplify this process a bit
    case SiriusSupervisor.IsInitializedRequest =>
      if (siriusStateAgent().areSubsystemsInitialized) {
        initSchedule.cancel()

        siriusStateAgent send (_.copy(supervisorInitialized = true))

        compactionManager = Some(childProvider.createCompactionManager())
        context.become(initialized)

        sender ! SiriusSupervisor.IsInitializedResponse(initialized = true)
      } else {
        sender ! SiriusSupervisor.IsInitializedResponse(initialized = false)
      }

    // Ignore other messages until Initialized.
    case _ =>
  }

  def initialized: Receive = {
    case get: Get => stateSup forward get
    case logQuery: LogQuery => stateSup forward logQuery
    case membershipMessage: MembershipMessage => membershipActor forward membershipMessage
    case SiriusSupervisor.IsInitializedRequest => sender ! new SiriusSupervisor.IsInitializedResponse(true)
    case statusQuery: StatusQuery => statusSubsystem forward statusQuery
    case compactionMessage: CompactionMessage => compactionManager match {
      case Some(actor) => actor forward compactionMessage
      case None =>
        logger.warning("Dropping {} cause CompactionMessage because CompactionManager is not up", compactionMessage.getClass.getName)
    }
    case CheckPaxosMembership =>
      if (membershipAgent.get().values.toSet.contains(Some(self))) {
        ensureOrderingActorRunning()
      } else {
        ensureOrderingActorStopped()
      }

    case paxosMessage: PaxosMessage => orderingActor match {
      case Some(actor) => actor forward paxosMessage
      case None =>
        logger.debug("Dropping {} PaxosMessage because Paxos is not up (yet?)", paxosMessage.getClass.getName)
    }
    case orderedReq: NonCommutativeSiriusRequest => orderingActor match {
      case Some(actor) => actor forward orderedReq
      case None =>
        logger.debug("Dropping {} because Paxos is not up (yet?)", orderedReq)
    }

    case Terminated(terminated) =>
      orderingActor match {
        case Some(actor) if actor == terminated => orderingActor = None
        case _ =>
      }

    case unknown: AnyRef => logger.warning("SiriusSupervisor Actor received unrecongnized message {}", unknown)
  }

  def ensureOrderingActorRunning() {
    orderingActor match {
      case Some(actorRef) =>
        // do nothing, already alive and kicking
      case _ =>
        val actor = childProvider.createPaxosSupervisor(membershipHelper, stateBridge ! _)
        context.watch(actor)
        orderingActor = Some(actor)
    }
  }

  def ensureOrderingActorStopped() {
    orderingActor match {
      case Some(actorRef) =>
        context.stop(actorRef)
      case _ =>
        // do nothing, if there's an actor it's already been terminated
    }
    orderingActor = None
  }

}
