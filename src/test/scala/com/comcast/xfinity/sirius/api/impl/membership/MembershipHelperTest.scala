package com.comcast.xfinity.sirius.api.impl.membership

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.actor.ActorRef
import collection.immutable

class MembershipHelperTest extends NiceTest {

  var membershipHelper: MembershipHelper = _

  before {
    membershipHelper = new MembershipHelper
  }

  describe("MembershipHelper") {
    describe("getRandomMember") {
      val localSiriusInfo = mock[SiriusInfo]
      val localActorRef = mock[ActorRef]
      val localMembershipData = new MembershipData(localActorRef)

      val remoteSiriusInfo = mock[SiriusInfo]
      val remoteActorRef = mock[ActorRef]
      val remoteMembershipData = new MembershipData(remoteActorRef)

      it("should send back a Member != the MembershipActor we asked...3 times in a row") {
        val membership = MembershipMap(remoteSiriusInfo -> remoteMembershipData, localSiriusInfo -> localMembershipData)

        val data = membershipHelper.getRandomMember(membership, localSiriusInfo)
        assert(data.get === MembershipData(remoteActorRef))

        val data2 = membershipHelper.getRandomMember(membership, localSiriusInfo)
        assert(data2.get === MembershipData(remoteActorRef))

        val data3 = membershipHelper.getRandomMember(membership, localSiriusInfo)
        assert(data3.get === MembershipData(remoteActorRef))
      }

      it("should send back a None if the only ActorRef in the MembershipMap is equal to the caller") {
        val membership = MembershipMap(localSiriusInfo -> MembershipData(localActorRef))

        val data = membershipHelper.getRandomMember(membership, localSiriusInfo)
        assert(data === None)
      }

      it("should send back a None if the membershipMap is empty") {
        val membership: MembershipMap = immutable.Map.empty

        val data = membershipHelper.getRandomMember(membership, localSiriusInfo)
        assert(data === None)
      }
    }
  }
}
