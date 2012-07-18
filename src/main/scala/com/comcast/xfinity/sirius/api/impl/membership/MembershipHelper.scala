package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.info.SiriusInfo
import util.Random

class MembershipHelper {
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
}