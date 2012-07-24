package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import org.junit.rules.TemporaryFolder
import scalax.file.Path
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.api.impl.membership._
import akka.actor.ActorSystem
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.{SiriusState, SiriusImpl}
import akka.testkit.TestActorRef
import annotation.tailrec

@RunWith(classOf[JUnitRunner])
class MembershipITest extends NiceTest {

  var sirius: SiriusImpl = _

  var actorSystem: ActorSystem = _

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

  var membershipAgent: Agent[MembershipMap] = _
  var stateAgent: Agent[SiriusState] = _
  var underTestActor: TestActorRef[MembershipActor] = _

  before {
    stageFiles()

    actorSystem = ActorSystem.create("Sirius")

    sirius = SiriusImpl
      .createSirius(new StringRequestHandler(), new DoNothingSiriusLog(), "localhost", 2552, clusterConfigFileName)
  }

  after {
    actorSystem.shutdown()
    tempFolder.delete()
    actorSystem.awaitTermination()
  }

  describe("a SiriusImpl") {
    it("updates its membershipMap after the cluster config file is changed and checkClusterConfig is invoked.") {
      assert(SiriusItestHelper.waitForInitialization(sirius), "Sirius took too long to initialize")

      assert(sirius.getMembershipMap.get.keys.exists("localhost:2552" == _))
      assert(sirius.getMembershipMap.get.keys.exists("localhost:2553" == _))
      assert(!sirius.getMembershipMap.get.keys.exists("localhost:2554" == _))

      //put some time between initial file update and subsequent update
      Thread.sleep(1000L)
      //update cluster config
      clusterConfigPath.append("localhost:2554\n")
      sirius.checkClusterConfig


      assert(waitForTrue(Any => {

        sirius.getMembershipMap.get.keys.exists("localhost:2554" == _)
      }, 1000L, 50L))

    }

  }

  @tailrec
  private def waitForTrue(test: (Any => Boolean), timeout: Long, waitBetween: Long): Boolean = {
    if (timeout < 0) {
      false

    } else if (test()) {
      true
    } else {
      Thread.sleep(waitBetween)
      waitForTrue(test, timeout - waitBetween, waitBetween)
    }

  }
}