package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorRef
import collection.immutable

class MembershipHelperTest extends NiceTest {

  var membershipHelper: MembershipHelper = _

  before {
    membershipHelper = new MembershipHelper
  }

  describe("MembershipHelper") {
    describe("getRandomMember") {
      val localSiriusId = "local:2552"
      val localActorRef = mock[ActorRef]
      val localMembershipData = new MembershipData(localActorRef)

      val remoteSiriusId = "remote:2552"
      val remoteActorRef = mock[ActorRef]
      val remoteMembershipData = new MembershipData(remoteActorRef)

      it("should send back a Member != the MembershipActor we asked...3 times in a row") {
        val membership = MembershipMap(remoteSiriusId -> remoteMembershipData, localSiriusId -> localMembershipData)

        val data = membershipHelper.getRandomMember(membership, localSiriusId)
        assert(data.get === MembershipData(remoteActorRef))

        val data2 = membershipHelper.getRandomMember(membership, localSiriusId)
        assert(data2.get === MembershipData(remoteActorRef))

        val data3 = membershipHelper.getRandomMember(membership, localSiriusId)
        assert(data3.get === MembershipData(remoteActorRef))
      }

      it("should send back a None if the only ActorRef in the MembershipMap is equal to the caller") {
        val membership = MembershipMap(localSiriusId -> MembershipData(localActorRef))

        val data = membershipHelper.getRandomMember(membership, localSiriusId)
        assert(data === None)
      }

      it("should send back a None if the membershipMap is empty") {
        val membership: MembershipMap = immutable.Map.empty

        val data = membershipHelper.getRandomMember(membership, localSiriusId)
        assert(data === None)
      }
    }
  }
}
