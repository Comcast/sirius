package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import collection.immutable
import akka.actor.{ActorSystem, ActorRef}
import org.scalatest.BeforeAndAfterAll
import akka.testkit.TestProbe

class MembershipHelperTest extends NiceTest with BeforeAndAfterAll {

  implicit val as = ActorSystem("MembershipHelperTest")

  override def afterAll {
    as.shutdown()
  }

  val membershipHelper: MembershipHelper = new MembershipHelper

  describe("MembershipHelper") {
    describe("getRandomMember") {
      val localActorRef = TestProbe().ref
      val localSirius = localActorRef

      val remoteActorRef = TestProbe().ref

      it("should send back a Member != the MembershipActor we asked...3 times in a row") {
        val membership =  Set(localActorRef, remoteActorRef)

        val data = membershipHelper.getRandomMember(membership, localSirius)
        assert(data.get === remoteActorRef)

        val data2 = membershipHelper.getRandomMember(membership, localSirius)
        assert(data2.get === remoteActorRef)

        val data3 = membershipHelper.getRandomMember(membership, localSirius)
        assert(data3.get === remoteActorRef)
      }

      it("should send back a None if the only ActorRef in the MembershipMap is equal to the caller") {
        val membership = Set(localActorRef)

        val data = membershipHelper.getRandomMember(membership, localSirius)
        assert(data === None)
      }

      it("should send back a None if the membershipMap is empty") {
        val membership = Set[ActorRef]()

        val data = membershipHelper.getRandomMember(membership, localSirius)
        assert(data === None)
      }
    }
  }
}
