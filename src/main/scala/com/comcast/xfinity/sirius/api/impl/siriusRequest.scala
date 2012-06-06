package com.comcast.xfinity.sirius.api.impl
import akka.actor.ActorRef
import com.comcast.xfinity.sirius.info.SiriusInfo

sealed trait SiriusRequest

case class Get(key: String) extends SiriusRequest
case class JoinCluster(nodeToJoint: Option[ActorRef], info: SiriusInfo) extends SiriusRequest // XXX: Is this the best place for this?
case class GetMembershipData() extends SiriusRequest

sealed trait NonIdempotentSiriusRequest extends SiriusRequest
case class Put(key: String, body: Array[Byte]) extends NonIdempotentSiriusRequest
case class Delete(key: String) extends NonIdempotentSiriusRequest
