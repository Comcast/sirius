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

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import com.typesafe.config.ConfigFactory
import java.util.{HashMap => JHashMap}
import akka.actor.{Actor, Props, ActorSystem}
import org.slf4j.LoggerFactory
import com.comcast.xfinity.sirius.api.SiriusConfiguration

object AkkaExternalAddressResolverITest {

  val logger = LoggerFactory.getLogger(AkkaExternalAddressResolverITest.getClass)

  class SuperSimpleActor extends Actor {
    def receive = {
      case any => logger.debug("{}", any)
    }
  }
}

class AkkaExternalAddressResolverITest extends NiceTest with BeforeAndAfterAll {

  import AkkaExternalAddressResolverITest._

  describe("an AkkaExternalAddressResolver configured to a remoting enabled system") {
    it ("must give the proper external address") {
      val configMap = new JHashMap[String, Any]()
      configMap.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider")
      configMap.put("akka.remote.transport", "akka.remote.netty.NettyRemoteTransport")
      configMap.put("akka.remote.netty.tcp.hostname", "127.0.0.1")
      configMap.put("akka.remote.netty.tcp.port", 2559)
      // this makes intellij not get mad
      val config = configMap.asInstanceOf[java.util.Map[String, _ <: AnyRef]]
      val siriusConfig = new SiriusConfiguration
      val actorSystem = ActorSystem("test-system", ConfigFactory.parseMap(config))
      siriusConfig.setProp(SiriusConfiguration.AKKA_EXTERNAL_ADDRESS_RESOLVER,AkkaExternalAddressResolver(actorSystem)(siriusConfig))
      try {
        val myActor = actorSystem.actorOf(Props[SuperSimpleActor], "myRef")

        val resolver = siriusConfig.getProp[AkkaExternalAddressResolver](SiriusConfiguration.AKKA_EXTERNAL_ADDRESS_RESOLVER).get
        assert("akka.tcp://test-system@127.0.0.1:2559/user/myRef" === resolver.externalAddressFor(myActor))

      } finally {
        actorSystem.shutdown()
      }
    }
  }

  describe("an AkkaExternalAddressResolver configured to a non-remoting enabled system") {
    it ("must give the original address") {
      val actorSystem = ActorSystem("test-system")
      try {
        val myActor = actorSystem.actorOf(Props[SuperSimpleActor], "myRef")
        val siriusConfig = new SiriusConfiguration
        siriusConfig.setProp(SiriusConfiguration.AKKA_EXTERNAL_ADDRESS_RESOLVER,AkkaExternalAddressResolver(actorSystem)(siriusConfig))
        val resolver = siriusConfig.getProp[AkkaExternalAddressResolver](SiriusConfiguration.AKKA_EXTERNAL_ADDRESS_RESOLVER).get
        assert("akka://test-system/user/myRef" === resolver.externalAddressFor(myActor))
      } finally {
        actorSystem.shutdown()
      }
    }
  }
}
