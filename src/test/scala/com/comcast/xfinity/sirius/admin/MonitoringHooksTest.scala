package com.comcast.xfinity.sirius.admin

import com.comcast.xfinity.sirius.api.SiriusConfiguration
import akka.actor.{ActorSystem, Actor}
import javax.management.{ObjectName, MBeanServer}
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import org.mockito.Mockito._
import org.mockito.Matchers.{eq => meq}
import akka.testkit.TestActorRef
import java.util.{HashMap => JHashMap, Hashtable => JHashtable}
import com.typesafe.config.ConfigFactory

object MonitoringHooksTest {

  class DummyMonitor {

  }

  class MonitoredActor(monitor: => Any, config: SiriusConfiguration) extends Actor with MonitoringHooks {
    def receive = {
      // ping/pong message to verify that the nod has finished preStart
      case 'register => registerMonitor(monitor, config)
      case 'unregister => unregisterMonitors(config)
    }
  }
}

class MonitoringHooksTest extends NiceTest with TimedTest {

  import MonitoringHooksTest._

  it ("should register all monitors as expected when registered to a local actor system," +
      "and properly clean up on exit, if we are configured to do so") {
    val mockMbeanServer = mock[MBeanServer]
    val siriusConfig = new SiriusConfiguration
    siriusConfig.setProp(SiriusConfiguration.MBEAN_SERVER, mockMbeanServer)

    implicit val actorSystem = ActorSystem("test-system")
    try {
      val monitor = new DummyMonitor
      val monitoredActor = TestActorRef(new MonitoredActor(monitor, siriusConfig), "test")

      val expectedObjectName = {
        val kvs = new JHashtable[String, String]
        kvs.put("host", "")
        kvs.put("port", "")
        kvs.put("sysname", "test-system")
        kvs.put("path", "/user/test")
        kvs.put("type", "DummyMonitor")
        new ObjectName("com.comcast.xfinity.sirius", kvs)
      }

      monitoredActor ! 'register
      verify(mockMbeanServer).registerMBean(meq(monitor), meq(expectedObjectName))

      monitoredActor ! 'unregister
      verify(mockMbeanServer).unregisterMBean(meq(expectedObjectName))

    } finally {
      actorSystem.shutdown()
    }
  }

  it ("should register all monitors as expected when registered to a remote enabled system," +
      "and properly clean up on exit, if we are configured to do so") {
    val mockMbeanServer = mock[MBeanServer]
    val siriusConfig = new SiriusConfiguration
    siriusConfig.setProp(SiriusConfiguration.MBEAN_SERVER, mockMbeanServer)

    val configMap = new JHashMap[String, Any]()
    configMap.put("akka.actor.provider", "akka.remote.RemoteActorRefProvider")
    configMap.put("akka.remote.transport", "akka.remote.netty.NettyRemoteTransport")
    configMap.put("akka.remote.netty.hostname", "127.0.0.1")
    configMap.put("akka.remote.netty.port", 2556)
    // this makes intellij not get mad
    val akkaConfig = configMap.asInstanceOf[java.util.Map[String, _ <: AnyRef]]

    implicit val actorSystem = ActorSystem("test-system", ConfigFactory.parseMap(akkaConfig))
    try {
      val monitor = new DummyMonitor
      val monitoredActor = TestActorRef(new MonitoredActor(monitor, siriusConfig), "test")

      val expectedObjectName = {
        val kvs = new JHashtable[String, String]
        kvs.put("host", "127.0.0.1")
        kvs.put("port", "2556")
        kvs.put("sysname", "test-system")
        kvs.put("path", "/user/test")
        kvs.put("type", "DummyMonitor")
        new ObjectName("com.comcast.xfinity.sirius", kvs)
      }

      monitoredActor ! 'register
      verify(mockMbeanServer).registerMBean(meq(monitor), meq(expectedObjectName))

      monitoredActor ! 'unregister
      verify(mockMbeanServer).unregisterMBean(meq(expectedObjectName))

    } finally {
      actorSystem.shutdown()
    }
  }

  it ("should do nothing if the MBeanServer is not configured") {
    implicit val actorSystem = ActorSystem("test-system")
    try {
      var wasCalled = false
      val monitoredActor = TestActorRef(
        new MonitoredActor(
          {wasCalled = true; new DummyMonitor},
          new SiriusConfiguration
        ), "test"
      )

      monitoredActor ! 'register
      assert(false === wasCalled)

    } finally {
      actorSystem.shutdown()
    }
  }
}