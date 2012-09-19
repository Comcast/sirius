package com.comcast.xfinity.sirius.api.impl.status

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import com.comcast.xfinity.sirius.admin.SiriusMonitorReader
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import org.mockito.Mockito._
import akka.testkit.{TestProbe, TestActorRef}
import com.comcast.xfinity.sirius.api.impl.status.NodeStats.{NodeConfig, MonitorStats, MemoryUsage, FullNodeStatus}
import akka.util.duration._

class StatusWorkerTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("StatusWorkerTest")

  override def afterAll() {
    actorSystem.shutdown()
  }

  describe("in response to a GetStatus message") {
    it ("must return everything it can") {
      val mockMonitorReader = mock[SiriusMonitorReader]
      val config = new SiriusConfiguration
      config.setProp("key1", "val1")
      config.setProp("key2", "val2")

      val underTest = TestActorRef(
        new StatusWorker(
          "akka://some-system@somehost:2552/user/sirius",
          config,
          mockMonitorReader
        )
      )

      doReturn(None).when(mockMonitorReader).getMonitorStats(config)

      val senderProbe = TestProbe()
      senderProbe.send(underTest, StatusWorker.GetStatus)

      senderProbe.expectMsgPF(3 seconds) {
        case FullNodeStatus(nodeName, _: MemoryUsage, configInfo, stats) =>
          assert(nodeName === "akka://some-system@somehost:2552/user/sirius")
          val stringifiedConfigMap = config.getConfigMap.map(kv => (kv._1, kv._2.toString))
          assert(NodeConfig(stringifiedConfigMap) === configInfo)
          assert(MonitorStats(None) === stats)

      }
    }
  }
}