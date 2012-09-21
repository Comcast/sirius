package com.comcast.xfinity.sirius.itest

import akka.actor.ActorSystem
import org.junit.rules.TemporaryFolder
import com.comcast.xfinity.sirius.writeaheadlog._
import scalax.file.Path
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.api.impl.{Put, OrderedEvent, SiriusImpl}
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.uberstore.UberStore
import java.io.File

@RunWith(classOf[JUnitRunner])
class BootstrapLogITest extends NiceTest with TimedTest {

  var sirius: SiriusImpl = _

  var actorSystem: ActorSystem = _

  val tempFolder = new TemporaryFolder()
  var logDir: File = _

  var stringRequestHandler: StringRequestHandler = _
  var clusterConfigPath: Path = _

  private def stageFiles() {
    tempFolder.create()
    stageLogFile()
    stageClusterConfigFile()
  }
  private def stageLogFile() {
    logDir = tempFolder.newFolder("uberstore")
    logDir.mkdirs()
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

    val logWriter = UberStore(logDir.getAbsolutePath)
    logWriter.writeEntry(OrderedEvent(1, 2, Put("first", "jam".getBytes)))
    logWriter.writeEntry(OrderedEvent(2, 3, Put("first", "jawn".getBytes)))

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