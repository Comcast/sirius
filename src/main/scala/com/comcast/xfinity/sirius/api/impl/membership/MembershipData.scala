package com.comcast.xfinity.sirius.api.impl.membership

import akka.actor.ActorRef

case class MembershipData(supervisorRef: ActorRef, paxosSupervisorRef: ActorRef)