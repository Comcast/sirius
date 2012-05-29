package com.comcast.xfinity.sirius.api.impl
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import akka.actor.Actor
import akka.actor.Props
import com.comcast.xfinity.sirius.api.impl.persistence.SiriusPersistenceActor
import com.comcast.xfinity.sirius.api.impl.state.SiriusStateActor
import com.comcast.xfinity.sirius.api.impl.paxos.SiriusPaxosActor
import com.comcast.xfinity.sirius.api.RequestHandler
import javax.management.ObjectName
import com.comcast.xfinity.sirius.writeaheadlog.LogWriter

/**
 * Supervisor actor for the set of actors needed for Sirius.
 */
class SiriusSupervisor(admin : SiriusAdmin, requestHandler : RequestHandler, logWriter : LogWriter) extends Actor {

  /* Startup child actors. */
  val stateActor = context.actorOf(Props(new SiriusStateActor(requestHandler)), "state")
  val persistenceActor = context.actorOf(Props(new SiriusPersistenceActor(stateActor, logWriter)), "persistence")
  val paxosActor = context.actorOf(Props(new SiriusPaxosActor(persistenceActor)), "paxos")
 
  override def preStart = {
    super.preStart()
    admin.registerMbeans()
  }
  
  override def postStop = {
    super.postStop()
    admin.unregisterMbeans()
  }
  
  def receive = {
    case any => println(any)
  }
}