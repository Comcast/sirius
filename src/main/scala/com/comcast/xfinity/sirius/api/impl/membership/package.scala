package com.comcast.xfinity.sirius.api.impl

package object membership {
  type MembershipMap = Map[String, MembershipData]
  val MembershipMap = Map[String, MembershipData]_

  sealed trait MembershipMessage

  case object GetMembershipData extends MembershipMessage
  case object CheckClusterConfig extends MembershipMessage
}

