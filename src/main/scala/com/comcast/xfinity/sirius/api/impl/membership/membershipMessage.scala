package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.info.SiriusInfo

sealed trait MembershipMessage

case class Join(info: Map[SiriusInfo, MembershipData]) extends MembershipMessage

case class NewMember(info: Map[SiriusInfo, MembershipData]) extends MembershipMessage