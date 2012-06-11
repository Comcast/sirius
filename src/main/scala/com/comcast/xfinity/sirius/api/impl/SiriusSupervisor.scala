package com.comcast.xfinity.sirius.api.impl

import membership._
import org.slf4j.LoggerFactory
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.api.impl.paxos.SiriusPaxosActor
import com.comcast.xfinity.sirius.api.impl.persistence.SiriusPersistenceActor
import com.comcast.xfinity.sirius.api.impl.state.SiriusStateActor
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.writeaheadlog.LogWriter
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import com.comcast.xfinity.sirius.info.SiriusInfo

/**
 * Supervisor actor for the set of actors needed for Sirius.
 */
class SiriusSupervisor(admin: SiriusAdmin, requestHandler: RequestHandler, logWriter: LogWriter) extends Actor with AkkaConfig {
  private val logger = LoggerFactory.getLogger(classOf[SiriusSupervisor])

  /* Startup child actors. */
  private[impl] var stateActor = createStateActor(requestHandler)
  private[impl] var persistenceActor = createPersistenceActor(stateActor, logWriter)
  private[impl] var paxosActor = createPaxosActor(persistenceActor)
  private[impl] var membershipActor = createMembershipActor()

  override def preStart = {
    super.preStart()
    admin.registerMbeans()
  }

  override def postStop = {
    super.postStop()
    admin.unregisterMbeans()
  }

  def receive = {
    case put: Put => paxosActor forward put
    case get: Get => stateActor forward get
    case delete: Delete => paxosActor forward delete
    case membershipMessage: MembershipMessage => membershipActor forward membershipMessage
    case unknown: AnyRef => logger.warn("SiriusSupervisor Actor received unrecongnized message {}", unknown)
  }


  // hooks for testing
  private[impl] def createStateActor(theRequestHandler: RequestHandler) =
    context.actorOf(Props(new SiriusStateActor(theRequestHandler)), "state")

  private[impl] def createPersistenceActor(theStateActor: ActorRef, theLogWriter: LogWriter) =
    context.actorOf(Props(new SiriusPersistenceActor(stateActor, logWriter)), "persistence")

  private[impl] def createPaxosActor(persistenceActor: ActorRef) =
    context.actorOf(Props(new SiriusPaxosActor(persistenceActor)), "paxos")

  private[impl] def createMembershipActor() =
    context.actorOf(Props(new MembershipActor()), "membership")
}