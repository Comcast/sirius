package com.comcast.xfinity.sirius.api.impl.membership

import util.Random
import akka.actor.ActorRef
import akka.agent.Agent

object MembershipHelper {
  def apply(membership: Agent[Map[String, ActorRef]], localSiriusRef: ActorRef): MembershipHelper =
    new MembershipHelper(membership, localSiriusRef)
}

class MembershipHelper(val membershipAgent: Agent[Map[String, ActorRef]], val localSiriusRef: ActorRef) {

  /**
   * Get a random value from a map whose key does not equal localSiriusRef
   * @return Some(ActorRef), not matching actorToAvoid, or None if none such found
   */
  def getRandomMember: Option[ActorRef] = {
    val membership = membershipAgent()
    val viableChoices = membership.values.toSet - localSiriusRef

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
