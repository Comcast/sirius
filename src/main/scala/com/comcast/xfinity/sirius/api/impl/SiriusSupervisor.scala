package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.api.impl.persistence.SiriusPersistenceActor
import com.comcast.xfinity.sirius.api.impl.state.SiriusStateActor
import com.comcast.xfinity.sirius.api.impl.paxos.SiriusPaxosActor
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.writeaheadlog.LogWriter
import org.slf4j.LoggerFactory
import akka.pattern.ask
import actors.Future
import akka.actor.{ActorRef, Actor, Props}

/**
 * Supervisor actor for the set of actors needed for Sirius.
 */
class SiriusSupervisor(admin: SiriusAdmin, requestHandler: RequestHandler, logWriter: LogWriter) extends Actor with AkkaConfig {
  private val logger = LoggerFactory.getLogger(classOf[SiriusSupervisor])


  /* Startup child actors. */
  var stateActor = context.actorOf(Props(new SiriusStateActor(requestHandler)), "state")
  var persistenceActor = context.actorOf(Props(new SiriusPersistenceActor(stateActor, logWriter)), "persistence")
  var paxosActor = context.actorOf(Props(new SiriusPaxosActor(persistenceActor)), "paxos")

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
    case ("paxos", ref: ActorRef) => paxosActor = ref
    case ("persistence", ref: ActorRef) => persistenceActor = ref
    case ("state", ref: ActorRef) => stateActor = ref
    case _ => logger.warn("SiriusSupervisor Actor received unrecongnized message")
  }

}