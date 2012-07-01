package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import akka.dispatch.Await
import akka.util.duration._
import org.junit.rules.TemporaryFolder
import com.comcast.xfinity.sirius.writeaheadlog._
import com.comcast.xfinity.sirius.api.impl.{AkkaConfig, SiriusImpl}

class WriteAheadLogITest extends NiceTest with AkkaConfig {

  var sirius: SiriusImpl = _

  var actorSystem: ActorSystem = _

  val tempFolder = new TemporaryFolder()
  var logFilename: String = _

  var siriusLog: SiriusFileLog = _

  before {
    tempFolder.create()
    logFilename = tempFolder.newFile("sirius_wal.log").getAbsolutePath

    actorSystem = ActorSystem.create("Sirius")

    val logWriter: SiriusFileLog = new SiriusFileLog(logFilename, new WriteAheadLogSerDe())

    sirius = new SiriusImpl(new StringRequestHandler(), actorSystem, logWriter)
    assert(SiriusItestHelper.waitForInitialization(sirius), "Sirius took too long to initialize")

    siriusLog = new SiriusFileLog(logFilename, new WriteAheadLogSerDe())
  }

  after {
    actorSystem.shutdown()
    tempFolder.delete()
    actorSystem.awaitTermination()
  }

  /**
   * Convenience method that returns a List[LogData] containing the contents of a Log
   *
   */
  def readEntries(): List[LogData] = {

    siriusLog.foldLeft[List[LogData]](Nil)((a, c) => c :: a).reverse

  }

  describe("a Sirius Write Ahead Log") {
    it("should have 1 entry after a PUT") {
      Await.result(sirius.enqueuePut("1", "some body".getBytes), (5 seconds))

      val logEntries = readEntries()

      assert(1 === logEntries.size)
      assert("some body" === new String(logEntries(0).payload.get))
      assert("1" === logEntries(0).key)
    }
    it("should have 2 entries after 2 PUTs") {
      Await.result(sirius.enqueuePut("1", "some body".getBytes), (5 seconds))
      Await.result(sirius.enqueuePut("2", "some other body".getBytes), (5 seconds))

      val logEntries = readEntries()

      assert(2 === logEntries.size)
      assert("some body" === new String(logEntries(0).payload.get))
      assert("1" === logEntries(0).key)
      assert("some other body" === new String(logEntries(1).payload.get))
      assert("2" === logEntries(1).key)
    }
    it("should have a PUT and a DELETE entry after a PUT and a DELETE") {
      Await.result(sirius.enqueuePut("1", "some body".getBytes), (5 seconds))
      Await.result(sirius.enqueueDelete("1"), (5 seconds))

      val logEntries = readEntries()

      assert(2 === logEntries.size)

      assert("PUT" === logEntries(0).actionType)
      assert("some body" === new String(logEntries(0).payload.get))
      assert("1" === logEntries(0).key)

      assert("DELETE" === logEntries(1).actionType)
      assert("" === new String(logEntries(1).payload.get))
      assert("1" === logEntries(1).key)
    }

    it("should have 2 PUT entries after 2 PUTs and a GET") {
      Await.result(sirius.enqueuePut("1", "some body".getBytes), (5 seconds))
      Await.result(sirius.enqueuePut("2", "some other body".getBytes), (5 seconds))
      Await.result(sirius.enqueueGet("1"), (5 seconds))

      val logEntries = readEntries()

      assert(2 === logEntries.size)
      assert("some body" === new String(logEntries(0).payload.get))
      assert("1" === logEntries(0).key)
      assert("some other body" === new String(logEntries(1).payload.get))
      assert("2" === logEntries(1).key)
    }

  }
}