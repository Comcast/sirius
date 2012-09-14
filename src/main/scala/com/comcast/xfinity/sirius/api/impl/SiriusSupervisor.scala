package com.comcast.xfinity.sirius.api.impl

import membership._
import paxos.PaxosMessages.PaxosMessage
import paxos.NaiveOrderingActor
import persistence._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.agent.Agent
import akka.util.Duration
import java.util.concurrent.TimeUnit
import scalax.file.Path
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
    val stateSup: ActorRef
    val orderingActor: ActorRef
    val logRequestActor: ActorRef
    val membershipActor: ActorRef
    val statusSubsystem: ActorRef
    val membershipAgent: Agent[Set[ActorRef]]
    val siriusStateAgent: Agent[SiriusState]
  }

  /**
   * @param requestHandler User implemented RequestHandler.
   * @param siriusLog Interface into the Sirius persistent log.
   * @param config the SiriusConfiguration for this node
   */
  def apply(
    _requestHandler: RequestHandler,
    _siriusLog: SiriusLog,
    config: SiriusConfiguration): SiriusSupervisor = {

    // XXX: the following is EXTREMELY busy, it needs clean up, but
    //      im not sure what the best way to do this is. I know it's
    //      probably not popular, but if we don't test this with unit
    //      tests it can get much simpler, it's still covered by integration
    //      tests, so it may be ok...
    new SiriusSupervisor with DependencyProvider {
      val siriusStateAgent = Agent(new SiriusState)(context.system)
      val membershipAgent = Agent(Set[ActorRef]())(context.system)
      
      val stateSup = context.actorOf(Props(StateSup(_requestHandler, _siriusLog, siriusStateAgent)), "state")

      val membershipActor = {
        val clusterConfigPath = config.getProp[String](SiriusConfiguration.CLUSTER_CONFIG) match {
          case Some(path) => path
          case None => throw new IllegalArgumentException(SiriusConfiguration.CLUSTER_CONFIG + " is not configured")
        }
        context.actorOf(
          Props(
            new MembershipActor(membershipAgent, siriusStateAgent, Path.fromString(clusterConfigPath))
          ), "membership")
      }

      val logRequestActor = context.actorOf(Props(new LogRequestActor(100, _siriusLog, self, membershipAgent)(config)), "log")

      val orderingActor =
        if (config.getProp(SiriusConfiguration.USE_PAXOS, false)) {
          val siriusPaxosAdapter = new SiriusPaxosAdapter(
            membershipAgent,
            _siriusLog.getNextSeq,
            stateSup,
            logRequestActor,
            self,
            config
          )
          siriusPaxosAdapter.paxosSubSystem
        } else {
          context.actorOf(Props(new NaiveOrderingActor(stateSup, _siriusLog.getNextSeq)), "paxos")
        }

      val statusSubsystem = context.actorOf(
        Props(
          StatusWorker(
            AkkaExternalAddressResolver(context.system).externalAddressFor(self),
            config
          )
        )
      )
    }
  }
}

/**
 * Top level actor for the Sirius system.  
 * 
 * Don't use the constructor to construct this guy, use the companion object's apply.
 */
class SiriusSupervisor() extends Actor with AkkaConfig {
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
        context.become(initialized)

        siriusStateAgent send (_.copy(supervisorInitialized = true))

        initSchedule.cancel()

        membershipActor ! CheckClusterConfig

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
    case TransferComplete => logger.info("Log transfer complete")
    case transferFailed: TransferFailed => logger.info("Log transfer failed, reason: " + transferFailed.reason)
    case logRequestMessage: LogRequestMessage => logRequestActor forward logRequestMessage
    case unknown: AnyRef => logger.warning("SiriusSupervisor Actor received unrecongnized message {}", unknown)
  }

}
