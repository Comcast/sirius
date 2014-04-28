/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.api.impl.membership

import scala.util.{Try, Random}
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
   * Get a random non-local actor in the cluster.
   * @return successful actorRef if a remote actor exists, failure otherwise.
   */
  def getRandomMember: Try[ActorRef] = {
    val remoteActors = membershipAgent().values.flatten.filter(_ != localSiriusRef).toArray
    Try {
      require(remoteActors.size != 0)
      remoteActors(Random.nextInt(remoteActors.size))
    }
  }
}
