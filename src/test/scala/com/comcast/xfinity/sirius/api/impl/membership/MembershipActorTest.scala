package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.dispatch.Await._
import akka.util.duration._
import akka.pattern.ask

import org.mockito.Mockito._
import org.mockito.Matchers._

import akka.testkit.{TestActor, TestProbe, TestActorRef}
import akka.actor.{ActorRef, ActorSystem}
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.{AkkaConfig}
import akka.testkit.TestActor.AutoPilot
import scala.Option
import collection.immutable

class MembershipActorTest extends NiceTest with AkkaConfig {

  var actorSystem: ActorSystem = _

  var underTestActor: TestActorRef[MembershipActor] = _

  var siriusInfo: SiriusInfo = _
  var expectedMap: MembershipMap = _

  var membershipAgent: Agent[MembershipMap] = _

  before {
    siriusInfo = mock[SiriusInfo]

    actorSystem = ActorSystem("testsystem")
    membershipAgent = mock[Agent[MembershipMap]]

    underTestActor = TestActorRef(new MembershipActor(membershipAgent, siriusInfo))(actorSystem)

    expectedMap = MembershipMap(siriusInfo -> MembershipData(underTestActor))
  }

  after {
    actorSystem.shutdown()
  }


  describe("a MembershipActor") {
    it("should add a new member to the membership map if it receives a AddMembers message") {
      val newMember = AddMembers(expectedMap)
      underTestActor ! newMember
      verify(membershipAgent).send(any(classOf[MembershipMap => MembershipMap]))

    }
    it("should report on cluster membership if it receives a GetMembershipData message") {
      when(membershipAgent()).thenReturn(expectedMap)
      val actualMembers = result((underTestActor ? GetMembershipData), (5 seconds)).asInstanceOf[MembershipMap]

      assert(expectedMap === actualMembers)
    }
    it("should tell peers, add to member map, and return a AddMembers if it receives a Join message") {
      val probe1 = new TestProbe(actorSystem)
      val probe2 = new TestProbe(actorSystem)
      val info1 = new SiriusInfo(1000, "cool-server")
      val info2 = new SiriusInfo(1000, "rad-server")

      val toAddProbe = new TestProbe(actorSystem)
      val toAddInfo = new SiriusInfo(1000, "local-server")
      val toAdd = MembershipMap(toAddInfo -> MembershipData(toAddProbe.ref))


      val membership = MembershipMap(info1 -> MembershipData(probe1.ref), info2 -> MembershipData(probe2.ref))
      when(membershipAgent()).thenReturn(membership)

      val newMember = result((underTestActor ? Join(toAdd)), (5 seconds)).asInstanceOf[AddMembers]
      //was map updated
      verify(membershipAgent).send(any(classOf[MembershipMap => MembershipMap]))

      //were peers notified
      probe1.expectMsg(AddMembers(toAdd))
      probe2.expectMsg(AddMembers(toAdd))
    }
    it("should update membership map when it receives AddMembers with a node that is already a member") {
      val coolServerProbe = new TestProbe(actorSystem)
      val coolServerInfo = new SiriusInfo(1000, "cool-server")

      val newerCoolServerProbe = new TestProbe(actorSystem)

      //stage membership map so that it includes coolserver
      val membership = MembershipMap(coolServerInfo -> MembershipData(coolServerProbe.ref), new SiriusInfo(2000, "rad-server") -> MembershipData(new TestProbe(actorSystem).ref))
      when(membershipAgent()).thenReturn(membership)

      //try add cool-server again but with updated MembershipData
      val toAdd = MembershipMap(coolServerInfo -> MembershipData(newerCoolServerProbe.ref))
      underTestActor ! AddMembers(toAdd)

      //was map updated
      verify(membershipAgent).send(any(classOf[MembershipMap => MembershipMap]))
    }
    it("should handle a Join message when the new node already is a member") {
      val coolServerInfo = new SiriusInfo(1000, "cool-server")
      val coolServerProbe = new TestProbe(actorSystem)

      val radServerInfo = new SiriusInfo(1000, "rad-server")
      val radServerProbe = new TestProbe(actorSystem)

      //stage membership with cool-server and rad-server
      val membership = MembershipMap(coolServerInfo -> MembershipData(coolServerProbe.ref), radServerInfo -> MembershipData(radServerProbe.ref))
      when(membershipAgent()).thenReturn(membership)

      val coolServerJoinMsg = Join(MembershipMap(coolServerInfo -> MembershipData(coolServerProbe.ref)))
      val addMembersMsg = result((underTestActor ? coolServerJoinMsg), (5 seconds)).asInstanceOf[AddMembers]

      assert(membership === addMembersMsg.member)

      //was map updated
      verify(membershipAgent).send(any(classOf[MembershipMap => MembershipMap]))

      //were peers notified
      coolServerProbe.expectMsg(AddMembers(coolServerJoinMsg.member))
      coolServerProbe.expectNoMsg((100 millis))
      radServerProbe.expectMsg(AddMembers(coolServerJoinMsg.member))
      radServerProbe.expectNoMsg((100 millis))
    }

    describe("when asked for a random member") {
      describe("from a GetRandomMember message") {
        it("should send back a Member != the MembershipActor we asked... 3 times in a row") {
          val coolServerProbe = new TestProbe(actorSystem)
          val coolServerInfo = new SiriusInfo(1000, "cool-server")

          val membership = MembershipMap(coolServerInfo -> MembershipData(coolServerProbe.ref),
                                         siriusInfo -> MembershipData(underTestActor.actorRef))
          when(membershipAgent()).thenReturn(membership)

          val data  = result((underTestActor ? GetRandomMember), (1 seconds)).asInstanceOf[MemberInfo]
          assert(data.member.get === MembershipData(coolServerProbe.ref))
          val data2 = result((underTestActor ? GetRandomMember), (1 seconds)).asInstanceOf[MemberInfo]
          assert(data2.member.get === MembershipData(coolServerProbe.ref))
          val data3 = result((underTestActor ? GetRandomMember), (1 seconds)).asInstanceOf[MemberInfo]
          assert(data3.member.get === MembershipData(coolServerProbe.ref))
        }
        it("should send back a None if the only ActorRef in the MembershipMap is equal to the caller") {
          val membership = MembershipMap(siriusInfo -> MembershipData(underTestActor))
          when(membershipAgent()).thenReturn(membership)

          val data = result((underTestActor ? GetRandomMember), (1 seconds)).asInstanceOf[MemberInfo]
          assert(data.member == None)
        }
        it("should send back a None if the membershipMap is empty") {
          val membership: MembershipMap = immutable.Map.empty
          when(membershipAgent()).thenReturn(membership)

          val data = result((underTestActor ? GetRandomMember), (1 seconds)).asInstanceOf[MemberInfo]
          assert(data.member == None)
        }
      }
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
                sender ! AddMembers(x ++ MembershipMap(info -> MembershipData(new TestProbe(actorSystem).ref)))
                Some(this)
              }
            }
          })


          underTestActor ! JoinCluster(Some(nodeToJoinProbe.ref), siriusInfo)
          nodeToJoinProbe.expectMsg(Join(expectedMap))
          //check if result of Join was added locally
          verify(membershipAgent).send(any(classOf[MembershipMap => MembershipMap]))


        }
      }
      describe("and is given no nodeToJoin") {
        it("should forward a NewMember message containing itself to the membershipActor") {
          val info = new SiriusInfo(100, "geeky-server")
          underTestActor ! JoinCluster(None, info)
          verify(membershipAgent).send(any(classOf[MembershipMap => MembershipMap]))

        }
      }

    }

  }
}