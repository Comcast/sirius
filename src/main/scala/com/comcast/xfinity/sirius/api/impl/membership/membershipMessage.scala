package com.comcast.xfinity.sirius.api.impl.membership
import com.comcast.xfinity.sirius.info.SiriusInfo

import akka.actor.ActorRef

sealed trait MembershipMessage

case class JoinCluster(nodeToJoin: Option[ActorRef], info: SiriusInfo) extends MembershipMessage

case class Join(member: Map[SiriusInfo, MembershipData]) extends MembershipMessage

case class AddMembers(member: Map[SiriusInfo, MembershipData]) extends MembershipMessage