package com.comcast.xfinity.sirius.api.impl

import bridge.PaxosStateBridge
import membership._
import paxos.PaxosMessages.PaxosMessage
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.agent.Agent
import akka.util.Duration
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

object SiriusSupervisor {

  sealed trait SupervisorMessage
  case object IsInitializedRequest extends SupervisorMessage

  case class IsInitializedResponse(initialized: Boolean)

  trait DependencyProvider {
    val siriusStateAgent: Agent[SiriusState]
    val membershipAgent: Agent[Set[ActorRef]]

    val stateSup: ActorRef
    val membershipActor: ActorRef
    val orderingActor: ActorRef
    val stateBridge: ActorRef
    val statusSubsystem: ActorRef
  }

  /**
   * @param _requestHandler User implemented RequestHandler.
   * @param _siriusLog Interface into the Sirius persistent log.
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

      val orderingActor = context.actorOf(
        Props(PaxosSup(membershipAgent, siriusLog.getNextSeq, stateBridge ! _, config)),
        "paxos"
      )

      val statusSubsystem = context.actorOf(
        Props(StatusWorker(AkkaExternalAddressResolver(context.system).externalAddressFor(self), config)),
        "status"
      )
    }
  }
}

/**
 * Top level actor for the Sirius system.  
 * 
 * Don't use the constructor to construct this guy, use the companion object's apply.
 */
class SiriusSupervisor extends Actor {
  this: SiriusSupervisor.DependencyProvider =>
  private val logger = Logging(context.system, "Sirius")

  val initSchedule = context.system.scheduler
    .schedule(Duration.Zero, Duration.create(50, TimeUnit.MILLISECONDS), self, SiriusSupervisor.IsInitializedRequest)

  override def postStop() {
    siriusStateAgent.close()
    membershipAgent.close()
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
    case orderedReq: NonCommutativeSiriusRequest => orderingActor forward orderedReq
    case get: Get => stateSup forward get
    case logQuery: LogQuery => stateSup forward logQuery
    case membershipMessage: MembershipMessage => membershipActor forward membershipMessage
    case paxosMessage: PaxosMessage => orderingActor forward paxosMessage
    case SiriusSupervisor.IsInitializedRequest => sender ! new SiriusSupervisor.IsInitializedResponse(true)
    case statusQuery: StatusQuery => statusSubsystem forward statusQuery
    case unknown: AnyRef => logger.warning("SiriusSupervisor Actor received unrecongnized message {}", unknown)
  }

}
