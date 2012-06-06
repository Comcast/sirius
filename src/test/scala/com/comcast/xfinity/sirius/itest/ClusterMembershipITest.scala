package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import org.junit.rules.TemporaryFolder
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.dispatch.Await._
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.membership.MembershipData

import com.comcast.xfinity.sirius.api.impl.{AkkaConfig, SiriusImpl}
import java.net.InetAddress

class ClusterMembershipITest extends NiceTest with AkkaConfig {

  var sirius: SiriusImpl = _
  var actorSystem: ActorSystem = _

  val tempFolder = new TemporaryFolder()
  var logFilename: String = _


  before {
    tempFolder.create
    logFilename = tempFolder.newFile("sirius_wal.log").getAbsolutePath

    actorSystem = ActorSystem.create("Sirius")

  }

  after {
    actorSystem.shutdown()
    tempFolder.delete()
    actorSystem.awaitTermination()
  }

  describe("SiriusImpl") {
    it("should by default create a cluster with only itself") {
      sirius = new SiriusImpl(new StringRequestHandler(), actorSystem, new DoNothingLogWriter())
      val membershipData = result(sirius.getMembershipMap, (5 seconds)).asInstanceOf[Map[SiriusInfo, MembershipData]]
      assert(1 === membershipData.size)
      assert(DEFAULT_PORT === membershipData.keySet.head.port)
      assert(InetAddress.getLocalHost().getHostName() === membershipData.keySet.head.hostName)

    }
    it("should attempt to join a cluster if supplied with a node to join")(pending)
  }

}