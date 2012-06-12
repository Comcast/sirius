package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.dispatch.Await._
import akka.util.duration._
import akka.pattern.ask
import akka.japi._

import org.mockito.Mockito._
import org.mockito.Matchers._

import akka.testkit.{TestActor, TestProbe, TestActorRef}
import akka.actor.{ActorRef, ActorSystem}
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.{Get, AkkaConfig}
import akka.testkit.TestActor.AutoPilot
import scala.Option

class MembershipActorTest extends NiceTest with AkkaConfig {

  var actorSystem: ActorSystem = _

  var underTestActor: TestActorRef[MembershipActor] = _

  var siriusInfo: SiriusInfo = _
  var expectedMap: Map[SiriusInfo, MembershipData] = _

  var membershipAgent: Agent[Map[SiriusInfo, MembershipData]] = _

  before {
    siriusInfo = mock[SiriusInfo]

    actorSystem = ActorSystem("testsystem")
    membershipAgent = mock[Agent[Map[SiriusInfo, MembershipData]]]

    underTestActor = TestActorRef(new MembershipActor(membershipAgent))(actorSystem)

    expectedMap = Map[SiriusInfo, MembershipData](siriusInfo -> MembershipData(underTestActor))
  }

  after {
    actorSystem.shutdown()
  }


  describe("a MembershipActor") {
    it("should add a new member to the membership map if it receives a AddMembers message") {
      val newMember = AddMembers(expectedMap)
      underTestActor ! newMember
      verify(membershipAgent).send(any(classOf[Map[SiriusInfo, MembershipData] => Map[SiriusInfo, MembershipData]]))

    }
    it("should report on cluster membership if it receives a GetMembershipData message") {
      when(membershipAgent()).thenReturn(expectedMap)
      val actualMembers = result((underTestActor ? GetMembershipData), (5 seconds)).asInstanceOf[Map[SiriusInfo, MembershipData]]

      assert(expectedMap === actualMembers)
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
      when(membershipAgent()).thenReturn(membership)

      val newMember = result((underTestActor ? Join(toAdd)), (5 seconds)).asInstanceOf[AddMembers]
      //was map updated
      verify(membershipAgent).send(any(classOf[Map[SiriusInfo, MembershipData] => Map[SiriusInfo, MembershipData]]))

      //were peers notified
      probe1.expectMsg(AddMembers(toAdd))
      probe2.expectMsg(AddMembers(toAdd))
    }
    it("should update membership map when it receives AddMembers with a node that is already a member") {
      val coolServerProbe = new TestProbe(actorSystem)
      val coolServerInfo = new SiriusInfo(1000, "cool-server")

      val newerCoolServerProbe = new TestProbe(actorSystem)

      //stage membership map so that it includes coolserver
      val membership = Map[SiriusInfo, MembershipData](coolServerInfo -> MembershipData(coolServerProbe.ref), new SiriusInfo(2000, "rad-server") -> MembershipData(new TestProbe(actorSystem).ref))
      when(membershipAgent()).thenReturn(membership)

      //try add cool-server again but with updated MembershipData
      val toAdd = Map[SiriusInfo, MembershipData](coolServerInfo -> MembershipData(newerCoolServerProbe.ref))
      underTestActor ! AddMembers(toAdd)

      //was map updated
      verify(membershipAgent).send(any(classOf[Map[SiriusInfo, MembershipData] => Map[SiriusInfo, MembershipData]]))
    }
    it("should handle a Join message when the new node already is a member") {
      val coolServerInfo = new SiriusInfo(1000, "cool-server")
      val coolServerProbe = new TestProbe(actorSystem)

      val radServerInfo = new SiriusInfo(1000, "rad-server")
      val radServerProbe = new TestProbe(actorSystem)

      //stage membership with cool-server and rad-server
      val membership = Map[SiriusInfo, MembershipData](coolServerInfo -> MembershipData(coolServerProbe.ref), radServerInfo -> MembershipData(radServerProbe.ref))
      when(membershipAgent()).thenReturn(membership)

      val coolServerJoinMsg = Join(Map[SiriusInfo, MembershipData](coolServerInfo -> MembershipData(coolServerProbe.ref)))
      val addMembersMsg = result((underTestActor ? coolServerJoinMsg), (5 seconds)).asInstanceOf[AddMembers]

      assert(membership === addMembersMsg.member)

      //was map updated
      verify(membershipAgent).send(any(classOf[Map[SiriusInfo, MembershipData] => Map[SiriusInfo, MembershipData]]))

      //were peers notified
      coolServerProbe.expectMsg(AddMembers(coolServerJoinMsg.member))
      coolServerProbe.expectNoMsg((100 millis))
      radServerProbe.expectMsg(AddMembers(coolServerJoinMsg.member))
      radServerProbe.expectNoMsg((100 millis))
    }


    //TODO: Verify this is supposed to be here
    describe("when receiving a JoinCluster message") {
      describe("and is given a nodeToJoin") {
        it("should send a Join message to nodeToJoin's ActorRef") {
          //setup TestProbes
          val nodeToJoinProbe = new TestProbe(actorSystem)
          val info = new SiriusInfo(100, "dorky-server")
          nodeToJoinProbe.setAutoPilot(new AutoPilot {
            def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
              case Join(x) => {
                sender ! AddMembers(x ++ Map[SiriusInfo, MembershipData](info -> MembershipData(new TestProbe(actorSystem).ref)))
                Some(this)
              }
            }
          })


          underTestActor ! JoinCluster(Some(nodeToJoinProbe.ref), siriusInfo)
          nodeToJoinProbe.expectMsg(Join(expectedMap))
          //check if result of Join was added locally
          verify(membershipAgent).send(any(classOf[Map[SiriusInfo, MembershipData] => Map[SiriusInfo, MembershipData]]))


        }
      }
      describe("and is given no nodeToJoin") {
        it("should forward a NewMember message containing itself to the membershipActor") {
          val info = new SiriusInfo(100, "geeky-server")
          underTestActor ! JoinCluster(None, info)
          verify(membershipAgent).send(any(classOf[Map[SiriusInfo, MembershipData] => Map[SiriusInfo, MembershipData]]))

        }
      }

    }

  }
}