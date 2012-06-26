package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.SiriusImpl
import akka.actor.ActorSystem
import akka.dispatch.Await
import akka.util.duration._
import org.junit.rules.TemporaryFolder
import com.comcast.xfinity.sirius.writeaheadlog._
import scalax.file.Path
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LogReplayITest extends NiceTest {

  var sirius: SiriusImpl = _

  var actorSystem: ActorSystem = _

  val tempFolder = new TemporaryFolder()
  var logFilename: String = _

  var siriusLog: SiriusFileLog = _
  var stringRequestHandler: StringRequestHandler = _
  
  before {
    tempFolder.create
    logFilename = tempFolder.newFile("sirius_wal.log").getAbsolutePath
    val path = Path.fromString(logFilename)
    path.append("ZXnHgnjaTQHEEwNVOo7wuw==|PUT|key|123|19700101T000012.345Z|QQ==\n");
    path.append("FcKBMsXg++2Z44UoYNnmSA==|PUT|key|123|19700101T000012.345Z|QXxB\n");
    
    actorSystem = ActorSystem.create("Sirius")

    val logWriter: SiriusFileLog = new SiriusFileLog(logFilename, new WriteAheadLogSerDe())

    stringRequestHandler = new StringRequestHandler()
    sirius = new SiriusImpl(stringRequestHandler, actorSystem, logWriter)

    siriusLog = new SiriusFileLog(logFilename, new WriteAheadLogSerDe())
  }

  after {
    actorSystem.shutdown()
    tempFolder.delete()
    actorSystem.awaitTermination()
  }
  
  /* XXX: we need a way to tell if the whole shebang is intizalised.
  describe("a Sirius Write Ahead Log") {
    it("blah") {
        assert(2 == stringRequestHandler.map.keySet.size)
    }
  }*/

}