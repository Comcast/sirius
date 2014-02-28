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
package com.comcast.xfinity.sirius.util

import akka.actor._
import com.comcast.xfinity.sirius.api.SiriusConfiguration

object AkkaExternalAddressResolver {
  def apply (system: ActorSystem)(siriusConfig: SiriusConfiguration): AkkaExternalAddressResolver ={
     new AkkaExternalAddressResolver(system.asInstanceOf[ExtendedActorSystem])(siriusConfig)
  }
}
/**
 * Class for figuring out a local actor refs remote address.  This is weird, and I got
 * the gist of it from the following guys:
 *
 *  <ul>
 *    <li>[[https://groups.google.com/forum/?fromgroups=#!searchin/akka-user/full\$20address/akka-user/POngjU9UpK8/wE74aYiWdWIJ]]</li>
 *    <li>[[http://doc.akka.io/docs/akka/snapshot/scala/serialization.html]]</li>
 *  </ul>
 *
 * Don't use directly, akka does some voodoo to make the ExtendedActorSystem available.
 *
 * Suggested usage is as follows:
 *
 *    AkkaExternalAddressResolver(actorSystem).externalAddressFor(actorRef)
 *
 */
class AkkaExternalAddressResolver(system: ExtendedActorSystem)(siriusConfig: SiriusConfiguration) {

  /**
   * The address of this ActorSystem, as seen externally
   */
  // the idea here is to discover our external address, by getting our address
  //  relative to an external host, "akka://@nohost:0"
  final val externalAddress = {
    val sslEnabled = siriusConfig.getProp(SiriusConfiguration.ENABLE_SSL,false)

    if(sslEnabled) {
      system.provider.getExternalAddressFor(new Address("akka.ssl.tcp", "", "nohost", 0))
    } else {
      system.provider.getExternalAddressFor(new Address("akka.tcp", "", "nohost", 0))
    }
  }

  /**
   * Return the String representation of the external path of the passed in ActorRef
   * if such exists, otherwise return the internal representation (no host/port or anything).
   *
   * This allegedly just works for Netty transport, but this is all we use.
   *
   * @param ref an ActorRef which must be local to the system on which this instance
   *          was created
   * @return the as externally of a representation of this actor as possible-
   *          full path with host and port if the local ActorSystem is set up with
   *          remoting, else the path locally.  Either way, this should be globally
   *          unique to all other paths this ActorRef can get to.
   */
  def externalAddressFor(ref: ActorRef): String = externalAddress match {
    case None => ref.path.toString
    case Some(addr) => ref.path.toStringWithAddress(addr)
  }
}
