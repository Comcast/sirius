package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.dispatch.Await._
import akka.util.duration._
import akka.pattern.ask
import com.comcast.xfinity.sirius.api.impl.{AkkaConfig, GetMembershipData}
import akka.testkit.{TestProbe, TestActorRef}

class MembershipActorTest extends NiceTest with AkkaConfig {

  var actorSystem: ActorSystem = _

  var underTestActor: TestActorRef[MembershipActor] = _

  var siriusInfo: SiriusInfo = _
  var expectedMap: Map[SiriusInfo, MembershipData] = _

  before {
    siriusInfo = mock[SiriusInfo]

    actorSystem = ActorSystem("testsystem")

    underTestActor = TestActorRef(new MembershipActor())(actorSystem)

    expectedMap = Map[SiriusInfo, MembershipData](siriusInfo -> MembershipData(underTestActor))
  }

  after {
    actorSystem.shutdown()
  }

  describe("a MembershipActor") {
    it("should add a new member to the membership map if it receives a NewMember message") {
      val newMember = NewMember(expectedMap)
      underTestActor ! newMember
      assert(expectedMap === underTestActor.underlyingActor.membershipMap)
    }
    it("should report on cluster membership if it receives a GetMembershipData message") {
      val members = result((underTestActor ? GetMembershipData()), (5 seconds)).asInstanceOf[Map[SiriusInfo, MembershipData]]
      assert(underTestActor.underlyingActor.membershipMap === members)
    }
    it("should tell peers, add to member map, and return a NewMembers if it receives a Join message") {
      val probe1 = new TestProbe(actorSystem)
      val probe2 = new TestProbe(actorSystem)
      val info1 = new SiriusInfo(1000, "cool-server")
      val info2 = new SiriusInfo(1000, "rad-server")

      val toAddProbe = new TestProbe(actorSystem)
      val toAddInfo = new SiriusInfo(1000, "local-server")
      val toAdd = Map[SiriusInfo, MembershipData] (toAddInfo -> MembershipData(toAddProbe.ref))


      val membership = Map[SiriusInfo, MembershipData](info1 -> MembershipData(probe1.ref), info2 -> MembershipData(probe2.ref))
      underTestActor.underlyingActor.membershipMap = membership

      val newMember = result((underTestActor ? Join(toAdd)), (5 seconds)).asInstanceOf[NewMember]
      //was map updated
      assert(3 === newMember.member.size)
      assert(toAddProbe.ref === newMember.member(toAddInfo).membershipActor)
      assert(underTestActor.underlyingActor.membershipMap === newMember.member)

      //where peers notified
      probe1.expectMsg(NewMember(toAdd))
      probe2.expectMsg(NewMember(toAdd))
    }

  }
}