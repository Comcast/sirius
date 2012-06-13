package com.comcast.xfinity.sirius.api.impl.membership

import org.slf4j.LoggerFactory

import com.comcast.xfinity.sirius.info.SiriusInfo

import akka.actor.{ActorRef, actorRef2Scala, Actor}

import akka.dispatch.Await

import akka.pattern.ask
import com.comcast.xfinity.sirius.api.impl._
import akka.agent.Agent

/**
 * Actor responsible for orchestrating request related to Sirius Cluster Membership
 */
class MembershipActor(membershipAgent: Agent[MembershipMap]) extends Actor with AkkaConfig {
  private val logger = LoggerFactory.getLogger(classOf[MembershipActor])


  def receive = {
    case JoinCluster(nodeToJoin, info) => nodeToJoin match {
      case Some(node: ActorRef) => {
        logger.debug(self + " joining " + node)
        //join node from a cluster
        val future = node ? Join(Map(info -> MembershipData(self)))
        val addMembers = Await.result(future, timeout.duration).asInstanceOf[AddMembers]
        //update our membership map
        addToLocalMembership(addMembers.member)
        logger.debug("added " + addMembers.member)
      }
      case None => addToLocalMembership(Map(info -> MembershipData(self)))
    }
    case Join(member) => {
      notifyPeers(member)
      addToLocalMembership(member)
      sender ! AddMembers(membershipAgent())
    }
    case AddMembers(member) => addToLocalMembership(member)
    case GetMembershipData => sender ! membershipAgent()
    case _ => logger.warn("Unrecognized message.")
  }

  /**
   * update local membership data structure.  If member already exists then overwrite it.
   */
  def addToLocalMembership(member: MembershipMap) = membershipAgent send (_ ++ member)


  /**
   * Notify existing members of the cluster that a new node has joined
   */
  def notifyPeers(newMember: MembershipMap) {
    membershipAgent().foreach {
      case (key, peerRef) => peerRef.membershipActor ! AddMembers(newMember)
    }
  }

}