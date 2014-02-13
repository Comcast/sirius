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

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import javax.management.ObjectName
import akka.actor.{Props, Actor, ActorSystem}
import java.util.{HashMap => JHashMap, Hashtable => JHashtable}
import com.typesafe.config.ConfigFactory

object ObjectNameHelperTest {

  class DummyMonitor {}

  object EmptyActor {
    def props: Props= {
      Props(classOf[EmptyActor])
    }
  }
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
      val actor = actorSystem.actorOf(EmptyActor.props, "test")

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
    configMap.put("akka.remote.netty.tcp.hostname", "127.0.0.1")
    configMap.put("akka.remote.netty.tcp.port", 2556)
    // this makes intellij not get mad
    val akkaConfig = configMap.asInstanceOf[java.util.Map[String, _ <: AnyRef]]
    implicit val actorSystem = ActorSystem("test-system", ConfigFactory.parseMap(akkaConfig))
    try {
      val actor = actorSystem.actorOf(EmptyActor.props, "test")

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
