package com.comcast.xfinity.sirius.api.impl.membership

import org.slf4j.LoggerFactory

import com.comcast.xfinity.sirius.info.SiriusInfo

import akka.actor.actorRef2Scala
import akka.actor.Actor

/**
 * Actor responsible for orchestrating request related to Sirius Cluster Membership
 */
class MembershipActor extends Actor {
  private val logger = LoggerFactory.getLogger(classOf[MembershipActor])

  var membershipMap = Map[SiriusInfo, MembershipData]() // TODO: Change to an Agent and pull out of Actor

  def receive = {
    case join: Join => {
      //TODO check if node(s) is/are already a member(s) and reject
      notifyPeers(join.info)
      updateLocalMembership(join.info)
      sender ! NewMember(membershipMap)
    }
    case newMember: NewMember => updateLocalMembership(newMember.info)
    case _ => logger.warn("Unrecognized message.")
  }

  /**
   * update local membership data structure
   */
  def updateLocalMembership(info: Map[SiriusInfo, MembershipData]) = membershipMap ++= info

  /**
   * Notify existing members of the cluster that a new node has joined
   */
  def notifyPeers(newMember: Map[SiriusInfo, MembershipData]) {
    membershipMap.foreach {
      case (key, peerRef) => peerRef.membershipActor ! NewMember(newMember)
    }
  }

}