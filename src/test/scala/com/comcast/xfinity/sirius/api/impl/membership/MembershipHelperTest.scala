package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import collection.immutable
import akka.actor.{ActorSystem, ActorRef}
import org.scalatest.BeforeAndAfterAll
import akka.testkit.TestProbe
import akka.agent.Agent

class MembershipHelperTest extends NiceTest with BeforeAndAfterAll {

  implicit val as = ActorSystem("MembershipHelperTest")

  override def afterAll {
    as.shutdown()
  }

  describe("MembershipHelper") {
    describe("getRandomMember") {
      val localActorRef = TestProbe().ref
      val remoteActorRef = TestProbe().ref

      it("should send back a Member != the MembershipActor we asked...3 times in a row") {
        val membership: Agent[Map[String, Option[ActorRef]]] =
          Agent(Map("local" -> Some(localActorRef), "remote" -> Some(remoteActorRef)))
        val membershipHelper: MembershipHelper = MembershipHelper(membership, localActorRef)

        val data = membershipHelper.getRandomMember
        assert(data.get === remoteActorRef)

        val data2 = membershipHelper.getRandomMember
        assert(data2.get === remoteActorRef)

        val data3 = membershipHelper.getRandomMember
        assert(data3.get === remoteActorRef)
      }

      it("should send back a None if the only ActorRef in the MembershipMap is equal to the caller") {
        val membership: Agent[Map[String, Option[ActorRef]]] =
          Agent(Map("local" -> Some(localActorRef)))
        val membershipHelper: MembershipHelper = MembershipHelper(membership, localActorRef)

        val data = membershipHelper.getRandomMember
        assert(data === None)
      }

      it("should send back a None if the membershipMap is empty") {
        val membership = Agent(Map[String, Option[ActorRef]]())
        val membershipHelper: MembershipHelper = MembershipHelper(membership, localActorRef)

        val data = membershipHelper.getRandomMember
        assert(data === None)
      }

      it("should send back a None if all values are currently None or local") {
        val membership = Agent(Map[String, Option[ActorRef]]("badactor" -> None, "similarlybad" -> None, "local" -> Some(localActorRef)))
        val membershipHelper: MembershipHelper = MembershipHelper(membership, localActorRef)

        val data = membershipHelper.getRandomMember
        assert(data === None)
      }
    }

    describe("getClusterInfo") {
      it("should return only members that have ActorRefs associated") {
        val (probe1, probe2) = (TestProbe(), TestProbe())
        val membership = Agent(Map[String, Option[ActorRef]](
          "nothere" -> None,
          "here" -> Some(probe1.ref),
          "there" -> Some(probe2.ref)
        ))
        val underTest = MembershipHelper(membership, TestProbe().ref)

        val activeMembers = underTest.getClusterInfo.activeMembers
        assert(2 === activeMembers.size)
        assert(activeMembers.contains(probe1.ref))
        assert(activeMembers.contains(probe2.ref))

      }
      it("should properly calculate simpleMajority for 0 members") {
        val membership = Agent(Map[String, Option[ActorRef]]())
        val underTest = MembershipHelper(membership, TestProbe().ref)

        assert(1 === underTest.getClusterInfo.simpleMajority)
      }
      it("should properly calculate simpleMajority for 1 members") {
        val membership = Agent(Map[String, Option[ActorRef]](
          "1" -> Some(TestProbe().ref)
        ))
        val underTest = MembershipHelper(membership, TestProbe().ref)

        assert(1 === underTest.getClusterInfo.simpleMajority)
      }
      it("should properly calculate simpleMajority for 2 members") {

        val membership = Agent(Map[String, Option[ActorRef]](
          "1" -> Some(TestProbe().ref),
          "2" -> Some(TestProbe().ref)
        ))
        val underTest = MembershipHelper(membership, TestProbe().ref)

        assert(2 === underTest.getClusterInfo.simpleMajority)
      }
      it("should properly calculate simpleMajority for 3 members") {

        val membership = Agent(Map[String, Option[ActorRef]](
          "1" -> Some(TestProbe().ref),
          "2" -> Some(TestProbe().ref),
          "3" -> Some(TestProbe().ref)
        ))
        val underTest = MembershipHelper(membership, TestProbe().ref)

        assert(2 === underTest.getClusterInfo.simpleMajority)
      }
    }
  }
}
