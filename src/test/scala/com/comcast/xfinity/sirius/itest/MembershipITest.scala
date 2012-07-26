package com.comcast.xfinity.sirius.itest

import org.junit.rules.TemporaryFolder
import scalax.file.Path
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.api.impl.SiriusImpl
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}

@RunWith(classOf[JUnitRunner])
class MembershipITest extends NiceTest with TimedTest {

  var sirius: SiriusImpl = _

  val tempFolder = new TemporaryFolder()
  var logFilename: String = _
  var clusterConfigFileName: String = _

  var stringRequestHandler: StringRequestHandler = _
  var clusterConfigPath: Path = _

  private def stageFiles() {
    tempFolder.create()
    stageClusterConfigFile()
  }

  private def stageClusterConfigFile() {
    clusterConfigFileName = tempFolder.newFile("cluster.conf").getAbsolutePath
    clusterConfigPath = Path.fromString(clusterConfigFileName)
    clusterConfigPath.append("akka://some-system@somehost:2552/user/actor1\n")
    clusterConfigPath.append("akka://some-system@somehost:2552/user/actor2\n")
  }

  before {
    stageFiles()

    sirius = SiriusImpl
      .createSirius(new StringRequestHandler(), new DoNothingSiriusLog(), "localhost", 2552, clusterConfigFileName)
  }

  after {
    sirius.shutdown()
    tempFolder.delete()
  }

  describe("a SiriusImpl") {
    it("updates its membershipMap after the cluster config file is changed and checkClusterConfig is invoked.") {
      assert(SiriusItestHelper.waitForInitialization(sirius), "Sirius took too long to initialize")

      val expected1 = sirius.actorSystem.actorFor("akka://some-system@somehost:2552/user/actor1")
      val expected2 = sirius.actorSystem.actorFor("akka://some-system@somehost:2552/user/actor2")
      assert(sirius.getMembership.get.contains(expected1))
      assert(sirius.getMembership.get.contains(expected2))

      //update cluster config
      clusterConfigPath.append("akka://some-system@somehost:2552/user/actor3\n")
      sirius.checkClusterConfig

      assert( waitForTrue(Any => {

        sirius.getMembership.get.contains(sirius.actorSystem.actorFor("akka://some-system@somehost:2552/user/actor3"))
      }, 1000L, 50L), "Membership map should contain new entry within a certain amount of time")
    }

  }


}
