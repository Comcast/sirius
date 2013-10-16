package com.comcast.xfinity.sirius.api.impl

import bridge.PaxosStateBridge
import com.comcast.xfinity.sirius.api.impl.membership.{MembershipHelper, MembershipActor}
import paxos.PaxosMessages.PaxosMessage
import akka.actor.{ActorContext, Actor, ActorRef, Props}
import akka.agent.Agent
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.paxos.{Replica, PaxosSup}
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
      Agent(new SiriusState)(context.system)

    def createMembershipAgent()(implicit context: ActorContext) =
      Agent(Set[ActorRef]())(context.system)

    def createStateSupervisor(stateAgent: Agent[SiriusState])(implicit context: ActorContext) =
      context.actorOf(Props(StateSup(requestHandler, siriusLog, stateAgent, config)), "state")

    def createMembershipActor(membershipAgent: Agent[Set[ActorRef]])(implicit context: ActorContext) =
      context.actorOf(Props(MembershipActor(membershipAgent, config)), "membership")

    def createStateBridge(stateSupervisor: ActorRef, siriusSupervisor: ActorRef, membershipHelper: MembershipHelper)
                         (implicit context: ActorContext) =
      context.actorOf(Props(
        PaxosStateBridge(siriusLog.getNextSeq, stateSupervisor, siriusSupervisor, membershipHelper, config)),
        "paxos-state-bridge")

    def createPaxosSupervisor(membershipAgent: Agent[Set[ActorRef]], performFun: Replica.PerformFun)(implicit context: ActorContext) =
      context.actorOf(Props(PaxosSup(membershipAgent, siriusLog.getNextSeq, performFun, config)), "paxos")

    def createStatusSubsystem(siriusSupervisor: ActorRef)(implicit context: ActorContext) = {
      val supervisorAddress = AkkaExternalAddressResolver(context.system).externalAddressFor(siriusSupervisor)
      context.actorOf(Props(StatusWorker(supervisorAddress, config)), "status")
    }

    def createCompactionManager()(implicit context: ActorContext) =
      context.actorOf(Props(new CompactionManager(siriusLog)(config)), "compactionManager")
  }

  /**
   * @param requestHandler User implemented RequestHandler.
   * @param siriusLog Interface into the Sirius persistent log.
   * @param config the SiriusConfiguration for this node
   */
  def apply(
    requestHandler: RequestHandler,
    siriusLog: SiriusLog,
    config: SiriusConfiguration): SiriusSupervisor = {
    new SiriusSupervisor(new ChildProvider(requestHandler, siriusLog, config))
  }
}

/**
 * Top level actor for the Sirius system.  
 * 
 * Don't use the constructor to construct this guy, use the companion object's apply.
 */
class SiriusSupervisor(childProvider: ChildProvider)(implicit config: SiriusConfiguration = new SiriusConfiguration) extends Actor {

  val siriusStateAgent = childProvider.createStateAgent()
  val membershipAgent = childProvider.createMembershipAgent()
  val stateSup = childProvider.createStateSupervisor(siriusStateAgent)
  val membershipActor = childProvider.createMembershipActor(membershipAgent)
  val stateBridge = childProvider.createStateBridge(stateSup, self, MembershipHelper(membershipAgent, self))
  val statusSubsystem = childProvider.createStatusSubsystem(self)
  var orderingActor: Option[ActorRef] = None
  var compactionManager : Option[ActorRef] = None


  private val logger = Logging(context.system, "Sirius")

  val initSchedule = context.system.scheduler
    .schedule(0 seconds, 50 milliseconds, self, SiriusSupervisor.IsInitializedRequest)

  val checkIntervalSecs = config.getProp(SiriusConfiguration.PAXOS_MEMBERSHIP_CHECK_INTERVAL, 2.0)
  val membershipCheckSchedule = context.system.scheduler.
    schedule(0 seconds, checkIntervalSecs seconds, self, CheckPaxosMembership)

  override def postStop() {
    siriusStateAgent.close()
    membershipAgent.close()
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
    case CheckPaxosMembership => {
      if (membershipAgent.get().contains(self)) {
        ensureOrderingActorRunning()
      } else {
        ensureOrderingActorStopped()
      }
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
    case unknown: AnyRef => logger.warning("SiriusSupervisor Actor received unrecongnized message {}", unknown)
  }

  def ensureOrderingActorRunning() {
    orderingActor match {
      case Some(actorRef) if !actorRef.isTerminated =>
        // do nothing, already alive and kicking
      case _ =>
        orderingActor = Some(childProvider.createPaxosSupervisor(membershipAgent, stateBridge ! _))
    }
  }

  def ensureOrderingActorStopped() {
    orderingActor match {
      case Some(actorRef) if !actorRef.isTerminated =>
        context.stop(actorRef)
      case _ =>
        // do nothing, if there's an actor it's already been terminated
    }
    orderingActor = None
  }

}
