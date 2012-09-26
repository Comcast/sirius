package com.comcast.xfinity.sirius.admin

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import javax.management.ObjectName
import akka.actor.{Props, Actor, ActorSystem}
import java.util.{HashMap => JHashMap, Hashtable => JHashtable}
import com.typesafe.config.ConfigFactory

object ObjectNameHelperTest {

  class DummyMonitor {}

  class EmptyActor extends Actor {
    def receive = {
      case _ =>
    }
  }
}

class ObjectNameHelperTest extends NiceTest with BeforeAndAfterAll {

  import ObjectNameHelperTest._

  val underTest = new ObjectNameHelper

  it ("should create an ObjectName as expected using the actor and mbean " +
      "when the system is configured without remoting") {
    implicit val actorSystem = ActorSystem("test-system")
    try {
      val actor = actorSystem.actorOf(Props[EmptyActor], "test")

      val expectedObjectName = {
        val kvs = new JHashtable[String, String]
        kvs.put("host", "")
        kvs.put("port", "")
        kvs.put("sysname", "test-system")
        kvs.put("path", "/user/test")
        kvs.put("name", "DummyMonitor")
        new ObjectName("com.comcast.xfinity.sirius", kvs)
      }

      val actualObjectName = underTest.getObjectName(new DummyMonitor, actor, actorSystem)
      assert(expectedObjectName === actualObjectName)
    } finally {
      actorSystem.shutdown()
    }
  }

  it ("should create an ObjectName as expected using the actor and mbean " +
      "when the system is configured with remoting") {
    val configMap = new JHashMap[String, Any]()
    configMap.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider")
    configMap.put("akka.remote.transport", "akka.remote.netty.NettyRemoteTransport")
    configMap.put("akka.remote.netty.hostname", "127.0.0.1")
    configMap.put("akka.remote.netty.port", 2556)
    // this makes intellij not get mad
    val akkaConfig = configMap.asInstanceOf[java.util.Map[String, _ <: AnyRef]]
    implicit val actorSystem = ActorSystem("test-system", ConfigFactory.parseMap(akkaConfig))
    try {
      val actor = actorSystem.actorOf(Props[EmptyActor], "test")

      val expectedObjectName = {
        val kvs = new JHashtable[String, String]
        kvs.put("host", "127.0.0.1")
        kvs.put("port", "2556")
        kvs.put("sysname", "test-system")
        kvs.put("path", "/user/test")
        kvs.put("name", "DummyMonitor")
        new ObjectName("com.comcast.xfinity.sirius", kvs)
      }

      val actualObjectName = underTest.getObjectName(new DummyMonitor, actor, actorSystem)
      assert(expectedObjectName === actualObjectName)
    } finally {
      actorSystem.shutdown()
    }
  }
}