package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import org.mockito.Mockito._
import akka.agent.Agent
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import scalax.io.LongTraversable
import org.mockito.Matchers._
import akka.actor.{Props, Actor, ActorSystem, ActorRef}
import akka.testkit.{TestProbe, TestActorRef}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor._
import javax.management.{ObjectName, MBeanServer}
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import org.mockito.ArgumentCaptor
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor.{PingMembership, MembershipInfoMBean}

class MembershipActorTest extends NiceTest {

  var actorSystem: ActorSystem = _

  var underTestActor: TestActorRef[MembershipActor] = _

  var expectedSet: Set[ActorRef] = _
  var clusterConfigPath: Path = _
  var membershipAgent: Agent[Set[ActorRef]] = _

  var mbeanServer: MBeanServer = _

  before {
    actorSystem = ActorSystem("testsystem")

    mbeanServer = mock[MBeanServer]
    membershipAgent = mock[Agent[Set[ActorRef]]]
    clusterConfigPath = mock[Path]

    when(clusterConfigPath.lines(NewLine, false)).thenReturn(LongTraversable("dummyhost:8080"))

    implicit val config = new SiriusConfiguration
    config.setProp(SiriusConfiguration.MBEAN_SERVER, mbeanServer)
    underTestActor = TestActorRef(
      new MembershipActor(membershipAgent, clusterConfigPath, 120 seconds, 120 seconds, config)
    )(actorSystem)

    expectedSet = Set(underTestActor)
  }

  after {
    actorSystem.shutdown()
  }

  describe("a MembershipActor") {
    it("should report on cluster membership if it receives a GetMembershipData message") {
      val senderProbe = TestProbe()(actorSystem)

      when(membershipAgent()).thenReturn(expectedSet)

      senderProbe.send(underTestActor, GetMembershipData)
      senderProbe.expectMsg(expectedSet)
    }

    it("should attempt to read in the cluster configuration and set the MembershipMap") {
      // preStart will send a membership map to the membership agent
      verify(membershipAgent).send(any(classOf[Set[ActorRef]]))
    }

    it("should attempt to read in the cluster configuration when a CheckClusterConfig message is recieved") {
      verify(membershipAgent, times(1)).send(any(classOf[Set[ActorRef]]))

      when(clusterConfigPath.lines(NewLine, false)).thenReturn(
        LongTraversable(
          "/user/someactor1",
          "/user/someactor2")
      )
      underTestActor ! CheckClusterConfig
      verify(membershipAgent, times(2)).send(any(classOf[Set[ActorRef]]))
    }

    it("should correctly create a new membership map when given a cluster config") {
      when(clusterConfigPath.lines(NewLine, false)).thenReturn(
        LongTraversable(
          "/user/sirius1",
          "/user/sirius2")
      )
      val actor1 = actorSystem.actorOf(Props(new Actor { def receive = { case _ => }}), "sirius1")
      val actor2 = actorSystem.actorOf(Props(new Actor { def receive = { case _ => }}), "sirius2")

      val membership = underTestActor.underlyingActor.createMembership(clusterConfigPath)
      assert(membership.contains(actor1), actor1 + " missing from " + membership)
      assert(membership.contains(actor2), actor2 + " missing from " + membership)
    }

    it("should reply properly to a Ping") {
      val senderProbe = TestProbe()(actorSystem)

      senderProbe.send(underTestActor, Ping(30L))
      senderProbe.expectMsg(Pong(30L))
    }
    it("should properly update roundtrip maps on a Pong") {
      val senderProbe = TestProbe()(actorSystem)
      val senderPath = senderProbe.testActor.path.toString

      val senderProbe2 = TestProbe()(actorSystem)
      val senderPath2 = senderProbe2.testActor.path.toString

      val mbeanCaptor = ArgumentCaptor.forClass(classOf[Any])
      verify(mbeanServer).registerMBean(mbeanCaptor.capture(), any[ObjectName])
      val membershipInfo = mbeanCaptor.getValue.asInstanceOf[MembershipInfoMBean]

      senderProbe.send(underTestActor, Pong(System.currentTimeMillis() - 100L))

      assert(membershipInfo.getTimeSinceLastPingUpdate.keySet.size == 1)
      assert(membershipInfo.getTimeSinceLastPingUpdate.get(senderPath) != None)
      assert(membershipInfo.getMembershipRoundTrip(senderPath) >= 0)

      senderProbe2.send(underTestActor, Pong(System.currentTimeMillis() - 200L))

      assert(membershipInfo.getTimeSinceLastPingUpdate.keySet.size == 2)
      assert(membershipInfo.getTimeSinceLastPingUpdate.get(senderPath2) != None)
      assert(membershipInfo.getMembershipRoundTrip(senderPath2) >= 0)

    }
    it("should Ping all known members on a PingMembership") {
      val senderProbe = TestProbe()(actorSystem)
      val senderProbe2 = TestProbe()(actorSystem)
      val senderProbe3 = TestProbe()(actorSystem)

      when(membershipAgent.get()).thenReturn(Set(senderProbe.testActor, senderProbe2.testActor, senderProbe3.testActor))

      senderProbe.send(underTestActor, PingMembership)

      senderProbe.expectMsgClass(classOf[Ping])
      senderProbe2.expectMsgClass(classOf[Ping])
      senderProbe3.expectMsgClass(classOf[Ping])
    }
  }
}
