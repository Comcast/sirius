package com.comcast.xfinity.sirius.itest

import akka.actor.ActorSystem
import org.junit.rules.TemporaryFolder
import com.comcast.xfinity.sirius.writeaheadlog._

import com.comcast.xfinity.sirius.api.impl._
import java.util.concurrent.TimeUnit

import scalax.file.Path
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}


class WriteAheadLogITest extends NiceTest with AkkaConfig with TimedTest {

  var sirius: SiriusImpl = _

  var actorSystem: ActorSystem = _

  val tempFolder = new TemporaryFolder()
  var logFilename: String = _

  var siriusLog: SiriusFileLog = _
  var clusterConfigPath: Path = _

  before {
    tempFolder.create()
    logFilename = tempFolder.newFile("sirius_wal.log").getAbsolutePath

    val clusterConfigFileName = tempFolder.newFile("cluster.conf").getAbsolutePath
    clusterConfigPath = Path.fromString(clusterConfigFileName)

    actorSystem = ActorSystem.create("Sirius")

    val logWriter: SiriusFileLog = new SiriusFileLog(logFilename, new WriteAheadLogSerDe())

    sirius = new SiriusImpl(
      new StringRequestHandler(),
      logWriter,
      clusterConfigPath
    )(actorSystem)
    assert(waitForTrue(sirius.isOnline, 5000, 500), "Sirius took too long to boot (>5s)")

    siriusLog = new SiriusFileLog(logFilename, new WriteAheadLogSerDe())
  }

  after {
    actorSystem.shutdown()
    tempFolder.delete()
    actorSystem.awaitTermination()
  }

  /**
   * Convenience method that returns a List[OrderedEvent] containing the contents of a Log
   *
   */
  def readEntries(): List[OrderedEvent] =
    siriusLog.foldLeft[List[OrderedEvent]](Nil)((a, c) => c :: a).reverse

  describe("a Sirius Write Ahead Log") {
    it("should have 1 entry after a PUT") {
      sirius.enqueuePut("1", "some body".getBytes).get(1, TimeUnit.SECONDS)

      val logEntries = readEntries()

      assert(1 === logEntries.size)

      val put = logEntries(0).request.asInstanceOf[Put]
      assert("1" === put.key)
      assert("some body" === new String(put.body))
    }

    it("should have 2 entries after 2 PUTs") {
      sirius.enqueuePut("1", "some body".getBytes).get(1, TimeUnit.SECONDS)
      sirius.enqueuePut("2", "some other body".getBytes).get(1, TimeUnit.SECONDS)

      val logEntries = readEntries()

      assert(2 === logEntries.size)

      val put1 = logEntries(0).request.asInstanceOf[Put]
      assert("1" === put1.key)
      assert("some body" === new String(put1.body))

      val put2 = logEntries(1).request.asInstanceOf[Put]
      assert("2" === put2.key)
      assert("some other body" === new String(put2.body))
    }

    it("should have a PUT and a DELETE entry after a PUT and a DELETE") {
      sirius.enqueuePut("1", "some body".getBytes).get(1, TimeUnit.SECONDS)
      sirius.enqueueDelete("1").get(1, TimeUnit.SECONDS)

      val logEntries = readEntries()

      assert(2 === logEntries.size)

      val put = logEntries(0).request.asInstanceOf[Put]
      assert("1" === put.key)
      assert("some body" === new String(put.body))

      val delete = logEntries(1).request.asInstanceOf[Delete]
      assert("1" === delete.key)
    }

    it("should have 2 PUT entries after 2 PUTs and a GET") {
      sirius.enqueuePut("1", "some body".getBytes).get(1, TimeUnit.SECONDS)
      sirius.enqueuePut("2", "some other body".getBytes).get(1, TimeUnit.SECONDS)
      sirius.enqueueGet("1").get(1, TimeUnit.SECONDS)

      val logEntries = readEntries()

      assert(2 === logEntries.size)

      val put1 = logEntries(0).request.asInstanceOf[Put]
      assert("1" === put1.key)
      assert("some body" === new String(put1.body))

      val put2 = logEntries(1).request.asInstanceOf[Put]
      assert("2" === put2.key)
      assert("some other body" === new String(put2.body))
    }

  }
}