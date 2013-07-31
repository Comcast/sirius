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
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor._

class MembershipActorTest extends NiceTest {

  var actorSystem: ActorSystem = _

  var underTestActor: TestActorRef[MembershipActor] = _

  var expectedSet: Set[ActorRef] = _
  var clusterConfigPath: Path = _
  var membershipAgent: Agent[Set[ActorRef]] = _

  before {
    actorSystem = ActorSystem("testsystem")
    membershipAgent = mock[Agent[Set[ActorRef]]]
    clusterConfigPath = mock[Path]

    when(clusterConfigPath.lines(NewLine, false)).thenReturn(LongTraversable("dummyhost:8080"))

    underTestActor = TestActorRef(
      new MembershipActor(membershipAgent, clusterConfigPath)
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

  }
}
