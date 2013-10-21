package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import org.mockito.Mockito._
import akka.agent.Agent
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import org.mockito.Matchers._
import akka.actor.{ActorSystem, ActorRef}
import akka.testkit.{TestProbe, TestActorRef}
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor._
import javax.management.{ObjectName, MBeanServer}
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import org.mockito.ArgumentCaptor
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor.{PingMembership, MembershipInfoMBean}
import java.io.File

object MembershipActorTest {
  def createTempDir = {
    Thread.sleep(5)
    val tempDirName = "%s/membership-actor-test-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    new File(tempDirName)
  }
}

class MembershipActorTest extends NiceTest with TimedTest {

  def makeMembershipActor(clusterConfigContents: List[String] = List(),
                          membershipAgent: Agent[Map[String, ActorRef]] = Agent[Map[String, ActorRef]](Map())(actorSystem),
                          mbeanServer: MBeanServer = mock[MBeanServer]): (TestActorRef[MembershipActor], Agent[Map[String, ActorRef]]) = {

    val clusterConfigPath = new File(tempDir, "sirius.cluster.config").getAbsolutePath
    Path.fromString(clusterConfigPath)
      .writeStrings(clusterConfigContents, NewLine.sep)

    val config = new SiriusConfiguration
    config.setProp(SiriusConfiguration.MBEAN_SERVER, mbeanServer)
    config.setProp(SiriusConfiguration.MEMBERSHIP_CHECK_INTERVAL, 120)
    config.setProp(SiriusConfiguration.MEMBERSHIP_PING_INTERVAL, 120)
    config.setProp(SiriusConfiguration.CLUSTER_CONFIG, clusterConfigPath)
    val underTest = TestActorRef[MembershipActor](
      MembershipActor.props(membershipAgent, config), "membership-actor-test"
    )(actorSystem)

    (underTest, membershipAgent)
  }

  def writeClusterConfig(dir: File, lines: List[String]) {
    val clusterConfig = Path.fromString(new File(dir, "sirius.cluster.config").getAbsolutePath)
    clusterConfig.delete()
    clusterConfig.writeStrings(lines, NewLine.sep)
  }


  implicit var actorSystem: ActorSystem = _
  var tempDir: File = _

  before {
    actorSystem = ActorSystem("testsystem")
    tempDir = MembershipActorTest.createTempDir
  }

  after {
    Path.fromString(tempDir.getAbsolutePath).deleteRecursively(force = true)
    actorSystem.shutdown()
    waitForTrue(actorSystem.isTerminated, 1000, 50)
  }

  describe("a MembershipActor") {
    it("should report on cluster membership if it receives a GetMembershipData message") {
      val senderProbe = TestProbe()(actorSystem)
      val (underTest, _) = makeMembershipActor()

      senderProbe.send(underTest, GetMembershipData)
      senderProbe.expectMsgClass(classOf[Map[String, ActorRef]])
    }

    it("should add actors to membership when CheckClusterConfig is received") {
      val (underTest, membershipAgent) = makeMembershipActor()

      val probeOne = TestProbe()
      val probeOnePath = probeOne.ref.path.toString
      val probeTwo = TestProbe()
      val probeTwoPath = probeTwo.ref.path.toString

      writeClusterConfig(tempDir, List(probeOnePath, probeTwoPath))

      underTest ! CheckClusterConfig

      assert(waitForTrue(membershipAgent.get().size == 2, 2000, 100), "Did not reach correct membership size.")
      assert(probeOne.ref === membershipAgent.get()(probeOnePath))
      assert(probeTwo.ref === membershipAgent.get()(probeTwoPath))
    }

    /**
     * This test will be added when we move to akka 2.2. ActorRefs must be resolved before use.
     *
     * For now, they're instantiated whether or not the actor is running on the remote
     * system.
     */
    ignore("should remove actors from membership that fail to resolve") {
      val (underTest, membershipAgent) = makeMembershipActor()

      val probeOne = TestProbe()
      val probeOnePath = probeOne.ref.path.toString
      val probeTwo = TestProbe()
      val probeTwoPath = probeTwo.ref.path.toString

      writeClusterConfig(tempDir, List(probeOnePath, probeTwoPath))

      underTest ! CheckClusterConfig
      assert(waitForTrue(membershipAgent.get().size == 2, 2000, 100), "Did not reach correct membership size.")

      actorSystem.stop(probeTwo.ref)
      underTest ! CheckClusterConfig
      assert(waitForTrue(membershipAgent.get().size == 1, 2000, 100), "Did not remove stopped actor from membership.")
      assert(probeOne.ref === membershipAgent.get()(probeOnePath))
      assert(None === membershipAgent.get().get(probeTwoPath))
    }

    it("should remove actors from membership that were removed from cluster config") {
      val (underTest, membershipAgent) = makeMembershipActor()

      val probeOne = TestProbe()
      val probeOnePath = probeOne.ref.path.toString
      val probeTwo = TestProbe()
      val probeTwoPath = probeTwo.ref.path.toString

      writeClusterConfig(tempDir, List(probeOnePath, probeTwoPath))

      underTest ! CheckClusterConfig
      assert(waitForTrue(membershipAgent.get().size == 2, 2000, 100), "Did not reach correct membership size.")

      writeClusterConfig(tempDir, List(probeOnePath))

      underTest ! CheckClusterConfig

      assert(waitForTrue(membershipAgent.get().size == 1, 2000, 100), "Did not remove missing actorPath from membership.")
      assert(probeOne.ref === membershipAgent.get()(probeOnePath))
      assert(None === membershipAgent.get().get(probeTwoPath))
    }

    it("should reply properly to a Ping") {
      val senderProbe = TestProbe()(actorSystem)
      val (underTest, _) = makeMembershipActor()

      senderProbe.send(underTest, Ping(30L))
      senderProbe.expectMsg(Pong(30L))
    }

    it("should properly update roundtrip maps on a Pong") {
      val mockMbeanServer = mock[MBeanServer]
      val (underTest, _) = makeMembershipActor(mbeanServer = mockMbeanServer)

      val senderProbe = TestProbe()(actorSystem)
      val senderPath = senderProbe.testActor.path.toString

      val senderProbe2 = TestProbe()(actorSystem)
      val senderPath2 = senderProbe2.testActor.path.toString

      val mbeanCaptor = ArgumentCaptor.forClass(classOf[Any])
      verify(mockMbeanServer).registerMBean(mbeanCaptor.capture(), any[ObjectName])
      val membershipInfo = mbeanCaptor.getValue.asInstanceOf[MembershipInfoMBean]

      senderProbe.send(underTest, Pong(System.currentTimeMillis() - 100L))

      assert(membershipInfo.getTimeSinceLastPingUpdate.keySet.size == 1)
      assert(membershipInfo.getTimeSinceLastPingUpdate.get(senderPath) != None)
      assert(membershipInfo.getMembershipRoundTrip(senderPath) >= 0)

      senderProbe2.send(underTest, Pong(System.currentTimeMillis() - 200L))

      assert(membershipInfo.getTimeSinceLastPingUpdate.keySet.size == 2)
      assert(membershipInfo.getTimeSinceLastPingUpdate.get(senderPath2) != None)
      assert(membershipInfo.getMembershipRoundTrip(senderPath2) >= 0)

    }

    it("should Ping all known members on a PingMembership") {
      val senderProbe1 = TestProbe()(actorSystem)
      val senderProbe2 = TestProbe()(actorSystem)
      val senderProbe3 = TestProbe()(actorSystem)

      val (underTest, membershipAgent) = makeMembershipActor()

      membershipAgent send Map("1" -> senderProbe1.ref, "2" -> senderProbe2.ref, "3" -> senderProbe3.ref)
      waitForTrue(membershipAgent.get().size == 3, 200, 10)

      senderProbe1.send(underTest, PingMembership)

      senderProbe1.expectMsgClass(classOf[Ping])
      senderProbe2.expectMsgClass(classOf[Ping])
      senderProbe3.expectMsgClass(classOf[Ping])
    }
  }
}
