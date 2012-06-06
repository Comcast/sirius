package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.info.SiriusInfo

sealed trait MembershipMessage

case class Join(member: Map[SiriusInfo, MembershipData]) extends MembershipMessage

case class NewMember(member: Map[SiriusInfo, MembershipData]) extends MembershipMessage