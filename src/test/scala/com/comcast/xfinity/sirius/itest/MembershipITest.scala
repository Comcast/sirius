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
    clusterConfigPath.append("localhost:2552\n")
    clusterConfigPath.append("localhost:2553\n")
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

      assert(sirius.getMembershipMap.get.keySet.contains("localhost:2552"))
      assert(sirius.getMembershipMap.get.keySet.contains("localhost:2553"))
      assert(!sirius.getMembershipMap.get.keySet.contains("localhost:2554"))

      //update cluster config
      clusterConfigPath.append("localhost:2554\n")
      sirius.checkClusterConfig

      assert( waitForTrue(Any => {

        sirius.getMembershipMap.get.keySet.contains("localhost:2554")
      }, 1000L, 50L), "Membership map should contain new entry within a certain amount of time")
    }

  }


}
