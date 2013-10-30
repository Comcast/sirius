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
        val membership =  Agent(Map("local" -> localActorRef, "remote" -> remoteActorRef))
        val membershipHelper: MembershipHelper = MembershipHelper(membership, localActorRef)

        val data = membershipHelper.getRandomMember
        assert(data.get === remoteActorRef)

        val data2 = membershipHelper.getRandomMember
        assert(data2.get === remoteActorRef)

        val data3 = membershipHelper.getRandomMember
        assert(data3.get === remoteActorRef)
      }

      it("should send back a None if the only ActorRef in the MembershipMap is equal to the caller") {
        val membership = Agent(Map("local" -> localActorRef))
        val membershipHelper: MembershipHelper = MembershipHelper(membership, localActorRef)

        val data = membershipHelper.getRandomMember
        assert(data === None)
      }

      it("should send back a None if the membershipMap is empty") {
        val membership = Agent(Map[String, ActorRef]())
        val membershipHelper: MembershipHelper = MembershipHelper(membership, localActorRef)

        val data = membershipHelper.getRandomMember
        assert(data === None)
      }
    }

    describe("getClusterInfo") {
      it("should return only members that have ActorRefs associated") {
        val (probe1, probe2) = (TestProbe(), TestProbe())
        val membership = Agent(Map[String, ActorRef](
          "here" -> probe1.ref,
          "there" -> probe2.ref
        ))
        val underTest = MembershipHelper(membership, TestProbe().ref)

        val activeMembers = underTest.getClusterInfo.activeMembers
        assert(2 === activeMembers.size)
        assert(activeMembers.contains(probe1.ref))
        assert(activeMembers.contains(probe2.ref))

      }
      it("should properly calculate simpleMajority for 0 members") {
        val membership = Agent(Map[String, ActorRef]())
        val underTest = MembershipHelper(membership, TestProbe().ref)

        assert(1 === underTest.getClusterInfo.simpleMajority)
      }
      it("should properly calculate simpleMajority for 1 members") {
        val membership = Agent(Map[String, ActorRef](
          "1" -> TestProbe().ref
        ))
        val underTest = MembershipHelper(membership, TestProbe().ref)

        assert(1 === underTest.getClusterInfo.simpleMajority)
      }
      it("should properly calculate simpleMajority for 2 members") {

        val membership = Agent(Map[String, ActorRef](
          "1" -> TestProbe().ref,
          "2" -> TestProbe().ref
        ))
        val underTest = MembershipHelper(membership, TestProbe().ref)

        assert(2 === underTest.getClusterInfo.simpleMajority)
      }
      it("should properly calculate simpleMajority for 3 members") {

        val membership = Agent(Map[String, ActorRef](
          "1" -> TestProbe().ref,
          "2" -> TestProbe().ref,
          "3" -> TestProbe().ref
        ))
        val underTest = MembershipHelper(membership, TestProbe().ref)

        assert(2 === underTest.getClusterInfo.simpleMajority)
      }
    }
  }
}
