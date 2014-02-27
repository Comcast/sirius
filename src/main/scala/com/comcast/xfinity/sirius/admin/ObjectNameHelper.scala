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
package com.comcast.xfinity.sirius.admin

import akka.actor.{ActorSystem, ActorRef}
import javax.management.ObjectName
import java.util.{Hashtable => JHashtable}
import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver
import com.comcast.xfinity.sirius.api.SiriusConfiguration

class ObjectNameHelper {

  /**
   * Create a globally unique object name for an MBean/actor pair
   *
   * @param mbean the MBean to generate an ObjectName for, the resultant ObjectName will be
   *          unique for the MBean per unique actor
   * @param actor actor to generate an Object name for
   * @param actorSystem the actor system this actor was created on, used to derive the name
   *
   * @return an ObjectName globally unique with respect to mbean and actor
   */
  def getObjectName(mbean: Any, actor: ActorRef, actorSystem: ActorSystem)
                   (siriusConfig: SiriusConfiguration): ObjectName = {

    val akkaExternalAddressResolver = siriusConfig.getProp[AkkaExternalAddressResolver](SiriusConfiguration.AKKA_EXTERNAL_ADDRESS_RESOLVER).
        getOrElse(throw new IllegalStateException("SiriusConfiguration.AKKA_EXTERNAL_ADDRESS_RESOLVER returned nothing"))
    val kvs = new JHashtable[String, String]
    kvs.put("path", "/" + actor.path.elements.reduceLeft(_ + "/" + _))

    val (host, port) = getHostPort(actorSystem)(akkaExternalAddressResolver)
    kvs.put("host", host)
    kvs.put("port", port)
    kvs.put("sysname", actorSystem.name)

    val statClass = mbean.getClass.getSimpleName
    kvs.put("name", statClass)

    new ObjectName("com.comcast.xfinity.sirius", kvs)
  }

  private def getHostPort(actorSystem: ActorSystem)(akkaExternalAddressResolver: AkkaExternalAddressResolver): (String, String) =
    akkaExternalAddressResolver.externalAddress match {
        case None => ("", "")
        case Some(address) =>
          val host = address.host.getOrElse("")
          val port = address.port match {
            case None => ""
            case Some(portNo) => portNo.toString
          }
          (host, port)
      }
}
