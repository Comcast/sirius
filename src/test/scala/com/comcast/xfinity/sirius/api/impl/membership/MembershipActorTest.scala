package com.comcast.xfinity.sirius.api.impl.membership
import com.comcast.xfinity.sirius.NiceTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.actor.ActorRef

@RunWith(classOf[JUnitRunner])
class MembershipActorTest extends NiceTest {

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
    it("should report on cluster membership if it receives a GetMembership message")(pending)
    it("should tell peers, add to member map, and return a NewMembers if it receives a Join message")(pending)


  }
}