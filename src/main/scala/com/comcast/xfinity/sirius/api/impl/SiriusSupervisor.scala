package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.api.impl.persistence.SiriusPersistenceActor
import com.comcast.xfinity.sirius.api.impl.state.SiriusStateActor
import com.comcast.xfinity.sirius.api.impl.paxos.SiriusPaxosActor
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.writeaheadlog.LogWriter
import membership._
import org.slf4j.LoggerFactory
import akka.actor.{ActorRef, Actor, Props}
import akka.dispatch.{Await, Future}
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.pattern.ask

/**
 * Supervisor actor for the set of actors needed for Sirius.
 */
class SiriusSupervisor(admin: SiriusAdmin, requestHandler: RequestHandler, logWriter: LogWriter) extends Actor with AkkaConfig {
  private val logger = LoggerFactory.getLogger(classOf[SiriusSupervisor])


  /* Startup child actors. */
  private[impl] var stateActor = context.actorOf(Props(new SiriusStateActor(requestHandler)), "state")
  private[impl] var persistenceActor = context.actorOf(Props(new SiriusPersistenceActor(stateActor, logWriter)), "persistence")
  private[impl] var paxosActor = context.actorOf(Props(new SiriusPaxosActor(persistenceActor)), "paxos")
  private[impl] var membershipActor = context.actorOf(Props(new MembershipActor()), "membership")

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
    case joinCluster: JoinCluster => {
      joinCluster.nodeToJoint match {
        case Some(node: ActorRef) => {
          val future = node ? Join(Map(joinCluster.info -> MembershipData(membershipActor)))
          val clusterMembershipMap = Await.result(future, timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          membershipActor forward NewMember(clusterMembershipMap)
        }
        case None => membershipActor forward NewMember(Map(joinCluster.info -> MembershipData(membershipActor)))
      }
    }
    case _ => logger.warn("SiriusSupervisor Actor received unrecongnized message")
  }

}