package com.comcast.xfinity.sirius.api.impl.membership

/**
 * case class for passing MembershipData as a message, encapsulating
 * an Option so None is possible if no viable member exists.
 * @param member
 */
case class MemberInfo(member: Option[MembershipData])

