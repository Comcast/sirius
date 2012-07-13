package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import org.junit.rules.TemporaryFolder
import scalax.file.Path
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.api.impl.membership._
import akka.actor.{Props, ActorSystem}
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.{SiriusState, SiriusImpl}
import akka.util.Duration
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertTrue
import akka.testkit.TestActorRef
import org.mockito.Mockito._
import org.mockito.Matchers._

@RunWith(classOf[JUnitRunner])
class MembershipITest extends NiceTest {

  var sirius: SiriusImpl = _

  var actorSystem: ActorSystem = _

  val tempFolder = new TemporaryFolder()
  var logFilename: String = _

  var stringRequestHandler: StringRequestHandler = _
  var clusterConfigPath: Path = _

  private def stageFiles() {
    tempFolder.create()
    stageClusterConfigFile()
  }
  private def stageLogFile() {
    logFilename = tempFolder.newFile("sirius_wal.log").getAbsolutePath
    val path = Path.fromString(logFilename)
    path.append("38a3d11c36c4c4e1|PUT|key|123|19700101T000012.345Z|QQ==\n")
    path.append("8e8ca658d0c63868|PUT|key|123|19700101T000012.345Z|QXxB\n")
  }

  private def stageClusterConfigFile() {
    val clusterConfigFileName = tempFolder.newFile("cluster.conf").getAbsolutePath
    clusterConfigPath = Path.fromString(clusterConfigFileName)
    clusterConfigPath.append("host1:2552\n")
    clusterConfigPath.append("host2:2552\n")
  }

  var membershipAgent: Agent[MembershipMap] = _
  var stateAgent: Agent[SiriusState] = _
  var underTestActor: TestActorRef[MembershipActor] = _

  before {
    stageFiles()

    actorSystem = ActorSystem.create("Sirius")

    membershipAgent = Agent[MembershipMap](MembershipMap())(actorSystem)
    stateAgent = mock[Agent[SiriusState]]

    underTestActor = TestActorRef(new MembershipActor(membershipAgent, "local:2552", stateAgent, clusterConfigPath) {
      override val checkInterval: Duration = Duration.create(100, TimeUnit.MILLISECONDS)
    })(actorSystem)
  }

  after {
    actorSystem.shutdown()
    tempFolder.delete()
    actorSystem.awaitTermination()
  }

  describe("a MembershipActor") {
    it("updates its membership map after an alloted time from change on disk") {
      Thread.sleep(500L) // waiting for preStart to complete.

      assertTrue(membershipAgent.get().keys.exists("host1:2552" == _))
      assertTrue(membershipAgent.get().keys.exists("host2:2552" == _))

      clusterConfigPath.append("host3:2552\n")

      Thread.sleep(500L) // waiting for interval to expire.

      assertTrue(membershipAgent.get().keys.exists("host3:2552" == _))
    }

  }

}