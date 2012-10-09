package com.comcast.xfinity.sirius.api.impl

import bridge.PaxosStateBridge
import membership._
import paxos.PaxosMessages.PaxosMessage
import akka.actor.{Kill, Actor, ActorRef, Props}
import akka.agent.Agent
import akka.util.Duration
import akka.util.duration._
import java.util.concurrent.TimeUnit
import paxos.PaxosSup
import state.SiriusPersistenceActor.LogQuery
import state.StateSup
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import akka.event.Logging
import com.comcast.xfinity.sirius.api.{SiriusConfiguration, RequestHandler}
import status.StatusWorker
import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver
import status.StatusWorker.StatusQuery
import com.comcast.xfinity.sirius.api.impl.SiriusSupervisor.CheckPaxosMembership

object SiriusSupervisor {

  sealed trait SupervisorMessage
  case object IsInitializedRequest extends SupervisorMessage
  case object CheckPaxosMembership extends SupervisorMessage

  case class IsInitializedResponse(initialized: Boolean)

  trait DependencyProvider {
    val siriusStateAgent: Agent[SiriusState]
    val membershipAgent: Agent[Set[ActorRef]]

    val stateSup: ActorRef
    val membershipActor: ActorRef
    var orderingActor: Option[ActorRef]
    val stateBridge: ActorRef
    val statusSubsystem: ActorRef

    def ensureOrderingActorRunning()
    def ensureOrderingActorStopped()
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

    // XXX: the following is quite busy, it needs clean up, but
    //      im not sure what the best way to do this is. I know it's
    //      probably not popular, but if we don't test this with unit
    //      tests it can get much simpler, it's still covered by integration
    //      tests, so it may be ok...
    new SiriusSupervisor with DependencyProvider {
      val siriusStateAgent = Agent(new SiriusState)(context.system)
      val membershipAgent = Agent(Set[ActorRef]())(context.system)

      val stateSup = context.actorOf(
        Props(StateSup(requestHandler, siriusLog, siriusStateAgent, config)),
        "state"
      )

      val membershipActor = context.actorOf(
        Props(MembershipActor(membershipAgent, config)),
        "membership"
      )

      val stateBridge = context.actorOf(Props(
        PaxosStateBridge(siriusLog.getNextSeq, stateSup, self, MembershipHelper(membershipAgent, self), config)),
        "paxos-state-bridge"
      )

      var orderingActor: Option[ActorRef] = None

      val statusSubsystem = context.actorOf(
        Props(StatusWorker(AkkaExternalAddressResolver(context.system).externalAddressFor(self), config)),
        "status"
      )

      def ensureOrderingActorStopped() {
        orderingActor match {
          case Some(actorRef) if !actorRef.isTerminated =>
            context.stop(actorRef)
            orderingActor = None
          case _ =>
            orderingActor = None
        }
      }

      def ensureOrderingActorRunning() {
        orderingActor match {
          case Some(actorRef) if !actorRef.isTerminated =>
            // do nothing, already alive and kicking
          case _ =>
            orderingActor = Some(context.actorOf(
              Props(PaxosSup(membershipAgent, siriusLog.getNextSeq, stateBridge ! _, config)),
              "paxos"
            ))
        }
      }
    }
  }
}

/**
 * Top level actor for the Sirius system.  
 * 
 * Don't use the constructor to construct this guy, use the companion object's apply.
 */
class SiriusSupervisor(implicit config: SiriusConfiguration = new SiriusConfiguration) extends Actor {
  this: SiriusSupervisor.DependencyProvider =>
  private val logger = Logging(context.system, "Sirius")

  val initSchedule = context.system.scheduler
    .schedule(Duration.Zero, Duration.create(50, TimeUnit.MILLISECONDS), self, SiriusSupervisor.IsInitializedRequest)

  val checkIntervalSecs = config.getProp(SiriusConfiguration.PAXOS_MEMBERSHIP_CHECK_INTERVAL, 2.0)
  val membershipCheckSchedule = context.system.scheduler.
    schedule(0 seconds, checkIntervalSecs seconds, self, CheckPaxosMembership)

  override def postStop() {
    siriusStateAgent.close()
    membershipAgent.close()
    membershipCheckSchedule.cancel()
  }

  def receive = {
    case SiriusSupervisor.IsInitializedRequest =>
      if (siriusStateAgent().areSubsystemsInitialized) {
        initSchedule.cancel()

        siriusStateAgent send (_.copy(supervisorInitialized = true))

        context.become(initialized)

        sender ! new SiriusSupervisor.IsInitializedResponse(true)
      } else {
        sender ! new SiriusSupervisor.IsInitializedResponse(false)
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

}
