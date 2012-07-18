package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import akka.dispatch.Await._
import akka.util.duration._
import akka.pattern.ask
import org.mockito.Mockito._
import akka.actor.ActorSystem
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.{SiriusState, AkkaConfig}
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import scalax.io.LongTraversable
import org.mockito.Matchers._
import akka.testkit.{TestProbe, TestActorRef}

class MembershipActorTest extends NiceTest with AkkaConfig {

  var actorSystem: ActorSystem = _

  var underTestActor: TestActorRef[MembershipActor] = _
  var paxosProbe: TestProbe = _

  var expectedMap: MembershipMap = _
  var siriusStateAgent: Agent[SiriusState] = _
  var clusterConfigPath: Path = _
  var membershipAgent: Agent[MembershipMap] = _

  val localSiriusId: String = "local:2552"

  before {
    actorSystem = ActorSystem("testsystem")
    membershipAgent = mock[Agent[MembershipMap]]
    siriusStateAgent = mock[Agent[SiriusState]]
    clusterConfigPath = mock[Path]

    when(clusterConfigPath.lastModified).thenReturn(1L)
    when(clusterConfigPath.lines(NewLine, false)).thenReturn(LongTraversable("dummyhost:8080"))

    paxosProbe =  TestProbe()(actorSystem);
    underTestActor = TestActorRef(new MembershipActor(membershipAgent, localSiriusId,
      siriusStateAgent, clusterConfigPath))(actorSystem)

    expectedMap = MembershipMap(localSiriusId -> MembershipData(underTestActor, paxosProbe.ref))
  }

  after {
    actorSystem.shutdown()
  }

  describe("a MembershipActor") {
    it("should report on cluster membership if it receives a GetMembershipData message") {
      when(membershipAgent()).thenReturn(expectedMap)
      val actualMembers = result((underTestActor ? GetMembershipData), (5 seconds)).asInstanceOf[MembershipMap]

      assert(expectedMap === actualMembers)
    }

    it("should attempt to read in the cluster configuration and set the MembershipMap") {
      // preStart will send a membership map to the membership agent
      verify(membershipAgent).send(any(classOf[MembershipMap]))
    }

    it("should attempt to read in the cluster configuration when a CheckClusterConfig message is recieved") {
      verify(membershipAgent, times(1)).send(any(classOf[MembershipMap]))

      when(clusterConfigPath.lines(NewLine, false)).thenReturn(LongTraversable("dummyhost2:8080", "dummyhost:8080"))
      underTestActor.receive(CheckClusterConfig)
      verify(membershipAgent, times(2)).send(any(classOf[MembershipMap]))

    }

    it("should correctly create a new membership map when given a cluster config") {
      when(clusterConfigPath.lastModified).thenReturn(2L)
      when(clusterConfigPath.lines(NewLine, false)).thenReturn(LongTraversable("dummyhost2:8080", "dummyhost:8080"))
      val membershipMap = underTestActor.underlyingActor.createMembershipMap(clusterConfigPath)
      assert(true == membershipMap.keys.exists("dummyhost2:8080" == _))
      assert(true == membershipMap.keys.exists("dummyhost:8080" == _))
      assert(false == membershipMap.keys.exists("nodummyhost:8080" == _))
    }

  }
}