package com.comcast.xfinity.sirius.info

import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver
import akka.actor.{ActorSystem, ActorRef}

/**
 * Implementation of SiriusInfoMBean
 *
 * @param actorSystem used to derive supervisors address
 * @param supervisorRef ActorRef to the node supervisor
 */
class SiriusInfo(actorSystem: ActorSystem, supervisorRef: ActorRef) extends SiriusInfoMBean {

  val externalName = AkkaExternalAddressResolver(actorSystem).externalAddressFor(supervisorRef)

  def getNodeAddress: String = externalName

}