package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.dispatch.Await._
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.membership._

import com.comcast.xfinity.sirius.api.impl.{AkkaConfig, SiriusImpl}
import java.net.InetAddress
import akka.actor.ActorRef

class ClusterMembershipITest extends NiceTest with AkkaConfig {

  var sirius: SiriusImpl = _

  var siriusPort: Int = 2554


  before {
    sirius = SiriusImpl.createSirius(new StringRequestHandler(), new DoNothingSiriusLog, InetAddress.getLocalHost().getHostName(), siriusPort)
  }

  after {
    sirius.shutdown()
    siriusPort += 1 // looks like akka takes a little longer to let go of its port... to avoid timing issues just use different port.

  }

  describe("SiriusImpl") {
    it("should create a cluster with only itself if asked to join \"None\"") {
      joinSelf(sirius)

      //wait till join is complete
      var start = System.currentTimeMillis()
      var settled = false
      while (System.currentTimeMillis() <= start + 1000 && !settled) {
        if (sirius.membershipAgent.await(timeout).size == 1) {
          settled = true
        }
      }
      assert(settled, "took too long to Join(None) to complete")

      //ask sirius for membership
      val membershipData = result(sirius.getMembershipMap, (5 seconds)).asInstanceOf[MembershipMap]

      assert(1 === membershipData.size)
      assert(siriusPort === membershipData.keySet.head.port)
      assert(InetAddress.getLocalHost().getHostName() === membershipData.keySet.head.hostName)

    }
    it("two Sirius nodes should become a cluster when one sends another a JoinCluster message") {
      //cluster of One
      joinSelf(sirius)


      //create another Sirius and make it reqeust to join our original sirius node
      val anotherSirius = SiriusImpl.createSirius(new StringRequestHandler(), new DoNothingSiriusLog, InetAddress.getLocalHost().getHostName(), siriusPort + 1)
      val path = "akka://" + SYSTEM_NAME + "@" + InetAddress.getLocalHost().getHostName() + ":" + siriusPort + "/user/" + SUPERVISOR_NAME
      joinSelf(anotherSirius)
      anotherSirius.joinCluster(Some(sirius.actorSystem.actorFor(path)))

      //verify
      val start: Long = System.currentTimeMillis()
      var joinConfirmed = false
      while (System.currentTimeMillis() <= start + 1000 && !joinConfirmed) {
        val siriusMembership = result(sirius.getMembershipMap, (100 millis)).asInstanceOf[MembershipMap]
        val anotherSiriusMembership = result(anotherSirius.getMembershipMap, (100 millis)).asInstanceOf[MembershipMap]
        if (anotherSiriusMembership.size == siriusMembership.size && siriusMembership.size == 2) {
          joinConfirmed = true
        } else {
          Thread.sleep(50)
        }
      }
      assert(joinConfirmed, "Waited one second and cluster members could not confirm the join")
      anotherSirius.shutdown()

    }
  }


  def joinSelf(impl: SiriusImpl) {
    impl.joinCluster(None)
    //wait till join is complete
    var start = System.currentTimeMillis()
    var settled = false
    while (System.currentTimeMillis() <= start + 1000 && !settled) {
      if (impl.membershipAgent.await(timeout).size == 1) {
        settled = true
      }
    }
    assert(settled, "took too long to Join(None) to complete")
  }

}