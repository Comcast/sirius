package com.comcast.xfinity.sirius.api.impl.membership

import org.slf4j.LoggerFactory

import com.comcast.xfinity.sirius.info.SiriusInfo

import akka.actor.{ActorRef, actorRef2Scala, Actor}

import akka.dispatch.Await

import akka.pattern.ask
import com.comcast.xfinity.sirius.api.impl._
import akka.agent.Agent
// import persistence.InitiateTransfer
import util.Random

/**
 * Actor responsible for orchestrating request related to Sirius Cluster Membership
 */
class MembershipActor(membershipAgent: Agent[MembershipMap], siriusInfo: SiriusInfo) extends Actor with AkkaConfig {
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
    case GetRandomMember =>
      // self here is the MembershipActor we do NOT want to return, since the caller is asking us for a random member
      // (we can generally assume they want a different member than the one they called)
      val randomMember = getRandomMember(membershipAgent(), siriusInfo)
      sender ! MemberInfo(randomMember)
    // case initiateTransfer: InitiateTransfer => context.parent forward initiateTransfer
    case _ => logger.warn("Unrecognized message.")
  }

  /**
   * Get a random value from a map whose key does not equal via keyToAvoid
   * @param map membershipMap we're searching
   * @param keyToAvoid key that, if in the map, is not a candidate for choosing (generally
   *                   this MembershipActor's siriusInfo)
   * @return a MembershipData not matching keyToAvoid
   */
  def getRandomMember(map: MembershipMap, keyToAvoid: SiriusInfo): Option[MembershipData] = {
    val viableMap = map - keyToAvoid
    val keys = viableMap.keySet.toIndexedSeq
    // if there is nothing in the map OR vToAvoid is the only thing in the map, there is no viable member
    if (keys.isEmpty) {
      None
    } else {
      val random = Random.nextInt(keys.size)
      val v = map.get(keys(random))
      Some(v.get)
    }
  }

  /**
   * update local membership data structure.  If member already exists then overwrite it.
   */
  def addToLocalMembership(member: MembershipMap) { membershipAgent send (_ ++ member) }

  /**
   * Notify existing members of the cluster that a new node has joined
   */
  def notifyPeers(newMember: MembershipMap) {
    membershipAgent().foreach {
      case (key, peerRef) => peerRef.membershipActor ! AddMembers(newMember)
    }
  }

}
