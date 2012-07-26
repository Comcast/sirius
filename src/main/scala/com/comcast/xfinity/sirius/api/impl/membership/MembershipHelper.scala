package com.comcast.xfinity.sirius.api.impl.membership

import util.Random
import akka.actor.ActorRef

class MembershipHelper {

  /**
   * Get a random value from a map whose key does not equal via actorToAvoid
   * @param map membership we're searching
   * @param actorToAvoid an ActorRef that, if in the membership, is not a candidate for choosing (generally
   *                   this is the local sirius node, and when we're looking for a neighbor, we don't want
   *                   ourselves)
   * @return Some(ActorRef), not matching actorToAvoid, or None if none such found
   */
  def getRandomMember(membership: Set[ActorRef], actorToAvoid: ActorRef): Option[ActorRef] = {
    val viableChoices = membership - actorToAvoid

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
