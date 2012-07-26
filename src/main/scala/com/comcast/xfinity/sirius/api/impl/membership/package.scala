package com.comcast.xfinity.sirius.api.impl

package object membership {
  sealed trait MembershipMessage

  case object GetMembershipData extends MembershipMessage
  case object CheckClusterConfig extends MembershipMessage
}

