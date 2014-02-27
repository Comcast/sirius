/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.info

import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver
import akka.actor.{ActorSystem, ActorRef}
import com.comcast.xfinity.sirius.api.SiriusConfiguration

/**
 * Implementation of SiriusInfoMBean
 *
 * @param actorSystem used to derive supervisors address
 * @param supervisorRef ActorRef to the node supervisor
 */
class SiriusInfo(actorSystem: ActorSystem, supervisorRef: ActorRef,
                 akkaExternalAddressResolver: AkkaExternalAddressResolver) extends SiriusInfoMBean {
  val externalName = akkaExternalAddressResolver.externalAddressFor(supervisorRef)

  def getNodeAddress: String = externalName

}
