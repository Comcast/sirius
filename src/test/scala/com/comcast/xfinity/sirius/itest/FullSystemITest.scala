package com.comcast.xfinity.sirius.itest

import org.scalatest.BeforeAndAfterAll
import scalax.file.Path
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import com.comcast.xfinity.sirius.writeaheadlog.{SiriusLog, SiriusFileLog}
import com.comcast.xfinity.sirius.api.{Sirius, SiriusResult, RequestHandler}
import java.io.File
import com.comcast.xfinity.sirius.api.impl.persistence.LogRange
import java.util.concurrent.{CountDownLatch, TimeUnit}
import com.comcast.xfinity.sirius.api.impl.{NonCommutativeSiriusRequest, OrderedEvent, SiriusImpl}

object FullSystemITest {

  /**
   * Request handler for testing purposes that includes a countdown
   * latch so that we can monitor it for completion of events
   */
  class LatchedRequestHandler(expectedTicks: Int) extends RequestHandler {
    val latch = new CountDownLatch(expectedTicks)

    def handleGet(key: String): SiriusResult = SiriusResult.none()

    def handlePut(key: String, body: Array[Byte]): SiriusResult = {
      latch.countDown()
      SiriusResult.none()
    }

    def handleDelete(key: String): SiriusResult = {
      latch.countDown()
      SiriusResult.none()
    }

    def await(timeout: Long, timeUnit: TimeUnit) {
      latch.await(timeout, timeUnit)
    }
  }

  /**
   * A SiriusLog implementation storing all events in memory
   */
  class InMemoryLog extends SiriusLog {
    var entries = List[OrderedEvent]()

    def writeEntry(entry: OrderedEvent) {
      entries = entry :: entries
    }

    def createIterator(logRange: LogRange) = {
      throw new IllegalStateException("not implemented")
    }

    def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T = {
      entries.foldRight[T](acc0)(
        (event, acc) => foldFun(acc, event)
      )
    }
  }
}

class FullSystemITest extends NiceTest with TimedTest {

  import FullSystemITest._

  var tempDir: File = _

  var reqHandler1: LatchedRequestHandler = _
  var reqHandler2: LatchedRequestHandler = _
  var reqHandler3: LatchedRequestHandler = _

  var wal1: SiriusLog = _
  var wal2: SiriusLog = _
  var wal3: SiriusLog = _

  var sirius1: SiriusImpl = _
  var sirius2: SiriusImpl = _
  var sirius3: SiriusImpl = _

  before {
    // incredibly ghetto, but TemporaryFolder was doing some weird stuff...
    val tempDirName = "%s/sirius-fulltest-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    tempDir = new File(tempDirName)
    tempDir.mkdirs()

    // stage membership
    val membershipFile = new File(tempDir, "membership")
    Path.fromString(membershipFile.getAbsolutePath).append(
      "akka://sirius-system@localhost:42289/user/sirius\n" +
      "akka://sirius-system@localhost:42290/user/sirius\n" +
      "akka://sirius-system@localhost:42291/user/sirius\n"
    )

    // helper for booting up a sirius impl, it would be nice
    // to move this out once we abstract the actor system
    // stuff out of sirius
    def bootNode(handler: RequestHandler, wal: SiriusLog, port: Int) = {
      val sirius = SiriusImpl.createSirius(
        handler,
        wal,
        "localhost", port,
        membershipFile.getAbsolutePath,
        true
      )

      assert(doWaitForTrue(sirius.isOnline, 2000, 500),
        "Sirius Node failed to come up within alloted 2 seconds")

      sirius
    }

    // Spin up 3 implementations that will make up our cluster,
    // the port numbers match those of the membership
    reqHandler1 = new LatchedRequestHandler(3)
    wal1 = new InMemoryLog()
    sirius1 = bootNode(reqHandler1, wal1, 42289)

    reqHandler2 = new LatchedRequestHandler(3)
    wal2 = new InMemoryLog()
    sirius2 = bootNode(reqHandler2, wal2, 42290)

    reqHandler3 = new LatchedRequestHandler(3)
    wal3 = new InMemoryLog()
    sirius3 = bootNode(reqHandler3, wal3, 42291)
  }

  after {
    sirius1.shutdown()
    sirius2.shutdown()
    sirius3.shutdown()
    tempDir.delete()
  }


  it ("must reach a decision on a series of requests sent to a single node") {
    // Submit requests
    sirius1.enqueuePut("hello", "world".getBytes)
    sirius1.enqueueDelete("world")
    sirius1.enqueueDelete("yo")

    // Wait for the requests to hit all the handlers
    List(reqHandler1, reqHandler2, reqHandler3).foreach(_.await(2, TimeUnit.SECONDS))

    // this is pretty gross, but we don't really have a much better way atm, trevor
    //  has a patch in the works that will make this cool
    Thread.sleep(1000)

    // Timestamps on OrderedEvents are decided at Decision time, so they are not
    // consistent across nodes, they can/should probably be pushed down to be the
    // responsibility of the log
    def extractEvents(wal: SiriusLog) = wal.foldLeft(Set[(Long, NonCommutativeSiriusRequest)]()) {
      case (acc, OrderedEvent(seq, _, req)) => acc + Tuple2(seq, req)
    }
    val wal1Data = extractEvents(wal1)
    val wal2Data = extractEvents(wal2)
    val wal3Data = extractEvents(wal3)

    // transitive property
    assert(wal1Data === wal2Data, "Data from nodes 1 and 2 did not match")
    assert(wal2Data === wal3Data, "Data from nodes 2 and 3 did not match")

    // since wal1 == wal2 == wal3, we only need to check wal1's size
    assert(wal1Data.size == 3, "No data in logs")
  }

}