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
import akka.util.{Timeout, Duration}
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertTrue
import akka.testkit.TestActorRef
import org.mockito.Mockito._
import org.mockito.Matchers._
import annotation.tailrec

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
      override lazy val checkInterval: Duration = Duration.create(200, TimeUnit.MILLISECONDS)
    })(actorSystem)
  }

  after {
    actorSystem.shutdown()
    tempFolder.delete()
    actorSystem.awaitTermination()
  }

  @tailrec
  private def waitForTrue(test: (Any => Boolean), timeout: Long, waitBetween: Long): Boolean = {
    if (timeout < 0) {
      false
    } else if (test())
      true
    else {
      Thread.sleep(waitBetween)
      waitForTrue(test, timeout - waitBetween, waitBetween)
    }
  }

  describe("a MembershipActor") {
    it("updates its membership map after an alloted time from change on disk") {
      verify(stateAgent, timeout(1000).atLeastOnce).send(any(classOf[SiriusState => SiriusState]))

      assertTrue(membershipAgent.get().keySet.contains("host1:2552"))
      assertTrue(membershipAgent.get().keySet.contains("host2:2552"))

      Thread.sleep(1000) // wait a hot second before appending
      clusterConfigPath.append("host3:2552\n")

      assertTrue(waitForTrue(_ => membershipAgent.get().keySet.contains("host3:2552"), 3000L, 200))
    }
  }
}