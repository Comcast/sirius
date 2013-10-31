package com.comcast.xfinity.sirius.api.impl.membership

import util.Random
import akka.actor.ActorRef
import akka.agent.Agent
import scala.math.floor
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper.ClusterInfo

object MembershipHelper {
  case class ClusterInfo(activeMembers: Set[ActorRef], simpleMajority: Int)

  def apply(membership: Agent[Map[String, Option[ActorRef]]], localSiriusRef: ActorRef): MembershipHelper =
    new MembershipHelper(membership, localSiriusRef)
}

class MembershipHelper(val membershipAgent: Agent[Map[String, Option[ActorRef]]], val localSiriusRef: ActorRef) {

  /**
   * Get the current cluster summary. Contains active ActorRefs for membership, and
   * the number of nodes needed to achieve a simple majority.
   *
   * @return current ClusterInfo
   */
  def getClusterInfo = {
    val membership = membershipAgent.get()
    val activeMembers = membership.values.flatten.toSet
    val simpleMajority = floor((membership.keys.size / 2.0) + 1).toInt

    ClusterInfo(activeMembers, simpleMajority)
  }

  /**
   * Get a random value from a map whose key does not equal localSiriusRef
   * @return Some(ActorRef), not matching actorToAvoid, or None if none such found
   */
  def getRandomMember: Option[ActorRef] = {
    val membership = membershipAgent()
    val viableChoices = membership.values.flatten.filter(_ != localSiriusRef)

    // if there is nothing in the map OR keyToAvoid is the only thing in the map, there is no viable member
    if (viableChoices.isEmpty) {
      None
    } else {
      val random = chooseRandomValue(viableChoices.size)
      Some(viableChoices.toIndexedSeq(random))
    }
  }

  private[membership] def chooseRandomValue(size: Int): Int = {
    Random.nextInt(size)
  }
}
