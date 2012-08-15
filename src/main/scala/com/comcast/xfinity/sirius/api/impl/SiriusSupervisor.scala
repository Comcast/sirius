package com.comcast.xfinity.sirius.api.impl

import membership._
import org.slf4j.LoggerFactory
import paxos.PaxosMessages.PaxosMessage
import paxos.{ PaxosSup, Replica, NaiveOrderingActor }
import persistence._
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.writeaheadlog.{ SiriusFileLog, LogIteratorSource, SiriusLog }
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.agent.Agent
import akka.util.Duration
import java.util.concurrent.TimeUnit
import scalax.file.Path
import state.{ StateSup, SiriusStateActor }

object SiriusSupervisor {

  sealed trait SupervisorMessage
  case object IsInitializedRequest extends SupervisorMessage

  case class IsInitializedResponse(initialized: Boolean)

  trait DependencyProvider {
    val stateSup: ActorRef
    val orderingActor: ActorRef
    val logRequestActor: ActorRef
    val membershipActor: ActorRef
    val siriusStateAgent: Agent[SiriusState]
    val usePaxos: Boolean
  }

  /**
   * @param requestHandler User implemented RequestHandler.
   * @param siriusLog Interface into the Sirius persistent log.
   * @param siriusStateAgent Keeps track of the overall initialization state of Sirius.
   * @param membershipAgent Keeps track of the members of the Sirius cluster.
   * @param clusterConfigPath Path to a static file containing the cluster members. 
   * @param usePaxos True if we should use Paxos for ordering, false if we just use our naieve single node counter.
   */
  def apply(
    _requestHandler: RequestHandler,
    _siriusLog: SiriusLog,
    _siriusStateAgent: Agent[SiriusState],
    _membershipAgent: Agent[Set[ActorRef]],
    _clusterConfigPath: Path,
    _usePaxos: Boolean): SiriusSupervisor = {
    
    new SiriusSupervisor with DependencyProvider {
      val siriusStateAgent = _siriusStateAgent
      val usePaxos = _usePaxos
      
      val stateSup = context.actorOf(Props(StateSup(_requestHandler, _siriusLog, _siriusStateAgent)), "state")
      val membershipActor = context.actorOf(Props(new MembershipActor(_membershipAgent, _siriusStateAgent, _clusterConfigPath)), "membership")
      val logRequestActor = context.actorOf(Props(new LogRequestActor(100, _siriusLog, self, stateSup, _membershipAgent)), "log")
      val orderingActor = if (usePaxos) {
        val siriusPaxosAdapter = new SiriusPaxosAdapter(_membershipAgent, _siriusLog.getNextSeq, stateSup, logRequestActor)
        siriusPaxosAdapter.paxosSubSystem
      } else {
        context.actorOf(Props(new NaiveOrderingActor(stateSup, _siriusLog.getNextSeq)), "paxos")
      }
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
  private val logger = LoggerFactory.getLogger(classOf[SiriusSupervisor])

  val initSchedule = context.system.scheduler
    .schedule(Duration.Zero, Duration.create(50, TimeUnit.MILLISECONDS), self, SiriusSupervisor.IsInitializedRequest)

  def receive = {
    case SiriusSupervisor.IsInitializedRequest => {
      val siriusState = siriusStateAgent.get()
      val isStateActorInitialized = siriusState.stateActorState == SiriusState.StateActorState.Initialized
      val isMembershipActorInitialized = siriusState.membershipActorState == SiriusState.MembershipActorState
        .Initialized
      val isPersistenceInitialized = siriusState.persistenceState == SiriusState.PersistenceState.Initialized
      if (isStateActorInitialized && isMembershipActorInitialized && isPersistenceInitialized) {
        import context.become
        become(initialized)

        siriusStateAgent send ((state: SiriusState) => {
          state.updateSupervisorState(SiriusState.SupervisorState.Initialized)
        })

        initSchedule.cancel()
        membershipActor ! CheckClusterConfig
        sender ! new SiriusSupervisor.IsInitializedResponse(isPersistenceInitialized)
      }
    }

    // Ignore other messages until Initialized.
    case _ =>
  }

  def initialized: Receive = {
    case put: Put =>
      if (usePaxos) {
        // Paxos expects submit requests to kick off a round
        orderingActor forward PaxosSup.Submit(put)
      } else {
        orderingActor forward put
      }
    case delete: Delete =>
      if (usePaxos) {
        // Paxos expects submit requests to kick off a round
        orderingActor forward PaxosSup.Submit(delete)
      } else {
        orderingActor forward delete
      }
    case get: Get => stateSup forward get
    case membershipMessage: MembershipMessage => membershipActor forward membershipMessage
    case paxosMessage: PaxosMessage => orderingActor forward paxosMessage
    case SiriusSupervisor.IsInitializedRequest => sender ! new SiriusSupervisor.IsInitializedResponse(true)
    case TransferComplete => logger.info("Log transfer complete")
    case transferFailed: TransferFailed => logger.info("Log transfer failed, reason: " + transferFailed.reason)
    case logRequestMessage: LogRequestMessage => logRequestActor forward logRequestMessage
    case unknown: AnyRef => logger.warn("SiriusSupervisor Actor received unrecongnized message {}", unknown)
  }

}