package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.dispatch.Await._
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.membership.MembershipData

import com.comcast.xfinity.sirius.api.impl.{AkkaConfig, SiriusImpl}
import java.net.InetAddress

class ClusterMembershipITest extends NiceTest with AkkaConfig {

  var sirius: SiriusImpl = _

  var siriusPort: Int = 2554


  before {
    sirius = SiriusImpl.createSirius(new StringRequestHandler(), new DoNothingLogWriter, InetAddress.getLocalHost().getHostName(), siriusPort)
  }

  after {
    sirius.shutdown()
    siriusPort += 1 // looks like akka takes a little longer to let go of its port... to avoid timing issues just use different port.

  }

  describe("SiriusImpl") {
    it("should create a cluster with only itself if asked to join \"None\"") {
      sirius.joinCluster(None)
      val membershipData = result(sirius.getMembershipMap, (5 seconds)).asInstanceOf[Map[SiriusInfo, MembershipData]]
      assert(1 === membershipData.size)
      assert(siriusPort === membershipData.keySet.head.port)
      assert(InetAddress.getLocalHost().getHostName() === membershipData.keySet.head.hostName)

    }
    it("two Sirius nodes should become a cluster when one sends another a JoinCluster message") {
      //cluster of One
      sirius.joinCluster(None)


      //create another Sirius and make it reqeust to join our original sirius node
      val anotherSirius = SiriusImpl.createSirius(
          new StringRequestHandler(), new DoNothingLogWriter, InetAddress.getLocalHost().getHostName(), siriusPort + 1)
      val path = "akka://" + SYSTEM_NAME + "@" + InetAddress.getLocalHost().getHostName() + ":" + siriusPort + "/user/" + SUPERVISOR_NAME
      anotherSirius.joinCluster(Some(sirius.actorSystem.actorFor(path)))


      //verify
      val start: Long = System.currentTimeMillis()
      var joinConfirmed = false
      while (System.currentTimeMillis() <= start + 1000 && !joinConfirmed) {
        val siriusMembership = result(sirius.getMembershipMap, (5 seconds)).asInstanceOf[Map[SiriusInfo, MembershipData]]
        val anotherSiriusMembership = result(anotherSirius.getMembershipMap, (5 seconds)).asInstanceOf[Map[SiriusInfo, MembershipData]]
        if (anotherSiriusMembership.size == siriusMembership.size) {
          joinConfirmed = true
        } else {
          Thread.sleep(50)
        }
      }
      assert(joinConfirmed, "Waited one second and cluster members could not confirm the join")
      anotherSirius.shutdown()

    }
  }

}