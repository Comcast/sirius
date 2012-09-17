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
        val membership =  Agent(Set(localActorRef, remoteActorRef))
        val membershipHelper: MembershipHelper = MembershipHelper(membership, localActorRef)

        val data = membershipHelper.getRandomMember
        assert(data.get === remoteActorRef)

        val data2 = membershipHelper.getRandomMember
        assert(data2.get === remoteActorRef)

        val data3 = membershipHelper.getRandomMember
        assert(data3.get === remoteActorRef)
      }

      it("should send back a None if the only ActorRef in the MembershipMap is equal to the caller") {
        val membership = Agent(Set(localActorRef))
        val membershipHelper: MembershipHelper = MembershipHelper(membership, localActorRef)

        val data = membershipHelper.getRandomMember
        assert(data === None)
      }

      it("should send back a None if the membershipMap is empty") {
        val membership = Agent(Set[ActorRef]())
        val membershipHelper: MembershipHelper = MembershipHelper(membership, localActorRef)

        val data = membershipHelper.getRandomMember
        assert(data === None)
      }
    }
  }
}
