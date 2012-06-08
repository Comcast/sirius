package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.dispatch.Await._
import akka.util.duration._
import akka.pattern.ask
import akka.testkit.{TestProbe, TestActorRef}
import com.comcast.xfinity.sirius.api.impl.AkkaConfig
import com.comcast.xfinity.sirius.api.impl.GetMembershipData

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
    it("should add a new member to the membership map if it receives a AddMembers message") {
      val newMember = AddMembers(expectedMap)
      underTestActor ! newMember
      assert(expectedMap === underTestActor.underlyingActor.membershipMap)
    }
    it("should report on cluster membership if it receives a GetMembershipData message") {
      val members = result((underTestActor ? GetMembershipData()), (5 seconds)).asInstanceOf[Map[SiriusInfo, MembershipData]]
      assert(underTestActor.underlyingActor.membershipMap === members)
    }
    it("should tell peers, add to member map, and return a AddMembers if it receives a Join message") {
      val probe1 = new TestProbe(actorSystem)
      val probe2 = new TestProbe(actorSystem)
      val info1 = new SiriusInfo(1000, "cool-server")
      val info2 = new SiriusInfo(1000, "rad-server")

      val toAddProbe = new TestProbe(actorSystem)
      val toAddInfo = new SiriusInfo(1000, "local-server")
      val toAdd = Map[SiriusInfo, MembershipData](toAddInfo -> MembershipData(toAddProbe.ref))


      val membership = Map[SiriusInfo, MembershipData](info1 -> MembershipData(probe1.ref), info2 -> MembershipData(probe2.ref))
      underTestActor.underlyingActor.membershipMap = membership

      val newMember = result((underTestActor ? Join(toAdd)), (5 seconds)).asInstanceOf[AddMembers]
      //was map updated
      assert(3 === newMember.member.size)
      assert(toAddProbe.ref === newMember.member(toAddInfo).membershipActor)
      assert(underTestActor.underlyingActor.membershipMap === newMember.member)

      //were peers notified
      probe1.expectMsg(AddMembers(toAdd))
      probe2.expectMsg(AddMembers(toAdd))
    }
    it("should update membership map when it receives AddMembers with a node that is already a member") {
      val coolServerProbe = new TestProbe(actorSystem)
      val coolServerInfo = new SiriusInfo(1000, "cool-server")

      val newerCoolServerProbe = new TestProbe(actorSystem)

      //stage membership map so that it includes coolserver
      val membership = Map[SiriusInfo, MembershipData](coolServerInfo -> MembershipData(coolServerProbe.ref), new SiriusInfo(2000,"rad-server") -> MembershipData(new TestProbe(actorSystem).ref))
      underTestActor.underlyingActor.membershipMap = membership

      //try add cool-server again but with updated MembershipData
      val toAdd = Map[SiriusInfo, MembershipData](coolServerInfo -> MembershipData(newerCoolServerProbe.ref))
      underTestActor ! AddMembers(toAdd)

      //was map updated
      assert(2 === underTestActor.underlyingActor.membershipMap.size)
      assert(newerCoolServerProbe.ref === underTestActor.underlyingActor.membershipMap(coolServerInfo).membershipActor)
    }
    it ("should handle a Join message when the new node already is a member") {
      val coolServerInfo = new SiriusInfo(1000, "cool-server")
      val coolServerProbe = new TestProbe(actorSystem)

      val radServerInfo = new SiriusInfo(1000, "rad-server")
      val radServerProbe = new TestProbe(actorSystem)

      //stage membership with cool-server and rad-server
      val membership = Map[SiriusInfo, MembershipData](coolServerInfo -> MembershipData(coolServerProbe.ref), radServerInfo -> MembershipData(radServerProbe.ref))
      underTestActor.underlyingActor.membershipMap = membership

      val coolServerJoinMsg = Join(Map[SiriusInfo, MembershipData](coolServerInfo -> MembershipData(coolServerProbe.ref)))

      val addMembersMsg = result((underTestActor ? coolServerJoinMsg), (5 seconds)).asInstanceOf[AddMembers]
      //was map updated
      assert(2 === addMembersMsg.member.size)
      assert(coolServerProbe.ref === addMembersMsg.member(coolServerInfo).membershipActor)
      assert(radServerProbe.ref === addMembersMsg.member(radServerInfo).membershipActor)
      assert(underTestActor.underlyingActor.membershipMap === addMembersMsg.member)

      //were peers notified
      coolServerProbe.expectMsg(AddMembers(coolServerJoinMsg.member))
      coolServerProbe.expectNoMsg((100 millis))
      radServerProbe.expectMsg(AddMembers(coolServerJoinMsg.member))
      radServerProbe.expectNoMsg((100 millis))
    }


  }
}