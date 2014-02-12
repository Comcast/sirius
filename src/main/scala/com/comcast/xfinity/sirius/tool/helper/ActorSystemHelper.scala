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
package com.comcast.xfinity.sirius.tool.helper

import akka.actor.ActorSystem
import java.util.{HashMap => JHashMap}
import com.typesafe.config.ConfigFactory

/**
 * Helper object providing quick and dirty access to a singleton
 * ActorSystem.
 *
 * The methods on this object are not thread safe- it is meant
 * for usage within the tools package, which are all single threaded
 * helpers.
 */
object ActorSystemHelper {

  private var actorSystemOpt: Option[ActorSystem] = None

  /**
   * Get an ActorSystem, configured with remoting.
   *
   * Has the side effect of starting the ActorSystem if it
   * does not exist yet
   *
   * @return a reference to the ActorSystem contained in this object
   */
  def getActorSystem(): ActorSystem = {
    actorSystemOpt match {
      case Some(as) => as
      case None =>
        val as = ActorSystem("nodetool", makeConfig)
        actorSystemOpt = Some(as)
        as
    }
  }

  /**
   * Shut down the singleton ActorSystem contained within if it was
   * created.
   */
  def shutDownActorSystem() {
    actorSystemOpt match {
      case Some(as) => as.shutdown()
      case None => // no-op
    }
  }

  // create configuration
  // TODO: make these overridable via system properties
  private def makeConfig = {
    val configMap = new JHashMap[String, Any]()
    configMap.put("akka.loglevel", "ERROR")
    configMap.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider")
    configMap.put("akka.remote.transport", "akka.remote.netty.NettyRemoteTransport")
    configMap.put("akka.remote.netty.hostname", "")
    configMap.put("akka.remote.netty.port", 62610)
    // making this huge so we can do stuff like tail a remote log
    configMap.put("akka.remote.netty.message-frame-size", "100 MiB")
    // this is just so that the intellij shuts up
    ConfigFactory.parseMap(configMap.asInstanceOf[JHashMap[String, _ <: AnyRef]])
  }
}
