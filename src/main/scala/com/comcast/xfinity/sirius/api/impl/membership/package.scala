package com.comcast.xfinity.sirius.api.impl

import akka.actor.ActorRef
import com.comcast.xfinity.sirius.info.SiriusInfo

package object membership {
  type MembershipMap = Map[SiriusInfo, MembershipData]
  val MembershipMap = Map[SiriusInfo, MembershipData]_

  sealed trait MembershipMessage

  case class JoinCluster(nodeToJoin: Option[ActorRef], info: SiriusInfo) extends MembershipMessage

  case class Join(member: MembershipMap) extends MembershipMessage

  case class AddMembers(member: MembershipMap) extends MembershipMessage

  case object GetMembershipData extends MembershipMessage
}

