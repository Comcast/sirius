package com.comcast.xfinity.sirius.util

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import com.typesafe.config.ConfigFactory
import java.util.{HashMap => JHashMap}
import akka.actor.{Actor, Props, ActorSystem}

object AkkaExternalAddressResolverITest {
  class SuperSimpleActor extends Actor {
    def receive = {
      case any => println(any)
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
      configMap.put("akka.remote.netty.hostname", "127.0.0.1")
      configMap.put("akka.remote.netty.port", 2559)
      // this makes intellij not get mad
      val config = configMap.asInstanceOf[java.util.Map[String, _ <: AnyRef]]

      val actorSystem = ActorSystem("test-system", ConfigFactory.parseMap(config))
      try {
        val myActor = actorSystem.actorOf(Props[SuperSimpleActor], "myRef")

        val resolver = AkkaExternalAddressResolver(actorSystem)
        assert("akka://test-system@127.0.0.1:2559/user/myRef" === resolver.externalAddressFor(myActor))

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

        val resolver = AkkaExternalAddressResolver(actorSystem)
        assert("akka://test-system/user/myRef" === resolver.externalAddressFor(myActor))
      } finally {
        actorSystem.shutdown()
      }
    }
  }
}