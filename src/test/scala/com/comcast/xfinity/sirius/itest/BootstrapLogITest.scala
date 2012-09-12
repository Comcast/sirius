package com.comcast.xfinity.sirius.itest

import akka.actor.ActorSystem
import org.junit.rules.TemporaryFolder
import com.comcast.xfinity.sirius.writeaheadlog._
import scalax.file.Path
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.api.impl.SiriusImpl
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import com.comcast.xfinity.sirius.api.SiriusConfiguration

@RunWith(classOf[JUnitRunner])
class BootstrapLogITest extends NiceTest with TimedTest {

  var sirius: SiriusImpl = _

  var actorSystem: ActorSystem = _

  val tempFolder = new TemporaryFolder()
  var logFilename: String = _

  var stringRequestHandler: StringRequestHandler = _
  var clusterConfigPath: Path = _

  private def stageFiles() {
    tempFolder.create()
    stageLogFile()
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

  before {
    stageFiles()

    actorSystem = ActorSystem.create("Sirius")

    val logWriter: SiriusFileLog = new SiriusFileLog(logFilename, new WriteAheadLogSerDe())

    stringRequestHandler = new StringRequestHandler()

    val config = new SiriusConfiguration
    config.setProp(SiriusConfiguration.CLUSTER_CONFIG, clusterConfigPath.path)

    sirius = SiriusImpl(
      stringRequestHandler,
      logWriter,
      config
    )(actorSystem)
    assert(waitForTrue(sirius.isOnline, 5000, 500), "Sirius took too long to boot (>5s)")
  }

  after {
    actorSystem.shutdown()
    tempFolder.delete()
    actorSystem.awaitTermination()
  }

  describe("a Sirius") {
    it("once started should have \"bootstrapped\" the contents of the wal") {
      assert(1 === stringRequestHandler.map.keySet.size)
      assert( 2 === stringRequestHandler.cmdsHandledCnt)
    }

  }

}