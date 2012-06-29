package com.comcast.xfinity.sirius.api.impl

import membership._
import org.slf4j.LoggerFactory
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.api.impl.paxos.SiriusPaxosActor
import com.comcast.xfinity.sirius.api.impl.persistence.SiriusPersistenceActor
import com.comcast.xfinity.sirius.api.impl.state.SiriusStateActor
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.agent.Agent
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.util.Duration
import java.util.concurrent.TimeUnit

/**
 * Supervisor actor for the set of actors needed for Sirius.
 */
class SiriusSupervisor(admin: SiriusAdmin,
                       requestHandler: RequestHandler,
                       siriusLog: SiriusLog,
                       siriusStateAgent: Agent[SiriusState],
                       membershipAgent: Agent[MembershipMap],
                       siriusInfo: SiriusInfo) extends Actor with AkkaConfig {

  private val logger = LoggerFactory.getLogger(classOf[SiriusSupervisor])

  /* Startup child actors. */
  private[impl] var stateActor = createStateActor(requestHandler)
  private[impl] var persistenceActor = createPersistenceActor(stateActor, siriusLog)
  private[impl] var paxosActor = createPaxosActor(persistenceActor)
  private[impl] var membershipActor = createMembershipActor(membershipAgent)

  override def preStart() {
    super.preStart()
    admin.registerMbeans()
  }

  override def postStop() {
    super.postStop()
    admin.unregisterMbeans()
  }

  val initSchedule = context.system.scheduler.schedule(
    Duration.Zero, Duration.create(50, TimeUnit.MILLISECONDS), self, SiriusSupervisor.IsInitializedRequest);

  def receive = {
    case SiriusSupervisor.IsInitializedRequest => {
      val siriusState = siriusStateAgent.get()
      val isInitialized = siriusState.persistenceState == SiriusState.PersistenceState.Initialized
      if (isInitialized) {

        import context.become
        become(initialized)

        siriusStateAgent send ((state: SiriusState) => {
          state.updateSupervisorState(SiriusState.SupervisorState.Initialized)
        })

        initSchedule.cancel();
      }
      sender ! new SiriusSupervisor.IsInitializedResponse(isInitialized)
    }

    // Ignore other messages until Initialized.
    case _ =>
  }

  def initialized: Receive = {
    case put: Put => paxosActor forward put
    case get: Get => stateActor forward get
    case delete: Delete => paxosActor forward delete
    case membershipMessage: MembershipMessage => membershipActor forward membershipMessage
    case SiriusSupervisor.IsInitializedRequest => sender ! new SiriusSupervisor.IsInitializedResponse(true)

    case unknown: AnyRef => logger.warn("SiriusSupervisor Actor received unrecongnized message {}", unknown)
  }

  // hooks for testing
  private[impl] def createStateActor(theRequestHandler: RequestHandler) =
    context.actorOf(Props(new SiriusStateActor(theRequestHandler)), "state")

  private[impl] def createPersistenceActor(theStateActor: ActorRef, theLogWriter: SiriusLog) =
    context.actorOf(Props(new SiriusPersistenceActor(stateActor, siriusLog, siriusStateAgent)), "persistence")

  private[impl] def createPaxosActor(persistenceActor: ActorRef) =
    context.actorOf(Props(new SiriusPaxosActor(persistenceActor)), "paxos")

  private[impl] def createMembershipActor(membershipAgent: Agent[MembershipMap]) =
    context.actorOf(Props(new MembershipActor(membershipAgent, siriusInfo)), "membership")
}

object SiriusSupervisor {
  sealed trait SupervisorMessage
  case object IsInitializedRequest extends SupervisorMessage
  case class IsInitializedResponse(initialized: Boolean)
}