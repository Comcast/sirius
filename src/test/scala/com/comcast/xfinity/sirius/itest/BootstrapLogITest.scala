package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import org.junit.rules.TemporaryFolder
import com.comcast.xfinity.sirius.writeaheadlog._
import scalax.file.Path
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.api.impl.SiriusImpl

@RunWith(classOf[JUnitRunner])
class BootstrapLogITest extends NiceTest {

  var sirius: SiriusImpl = _

  var actorSystem: ActorSystem = _

  val tempFolder = new TemporaryFolder()
  var logFilename: String = _

  var stringRequestHandler: StringRequestHandler = _

  private def stageLogFile() = {
    tempFolder.create()
    logFilename = tempFolder.newFile("sirius_wal.log").getAbsolutePath
    val path = Path.fromString(logFilename)
    path.append("38a3d11c36c4c4e1|PUT|key|123|19700101T000012.345Z|QQ==\n");
    path.append("8e8ca658d0c63868|PUT|key|123|19700101T000012.345Z|QXxB\n");
  }

  before {
    stageLogFile()

    actorSystem = ActorSystem.create("Sirius")

    val logWriter: SiriusFileLog = new SiriusFileLog(logFilename, new WriteAheadLogSerDe())

    stringRequestHandler = new StringRequestHandler()

    sirius = new SiriusImpl(stringRequestHandler, actorSystem, logWriter)
    assert(SiriusItestHelper.waitForInitialization(sirius), "Sirius took too long to initialize")

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