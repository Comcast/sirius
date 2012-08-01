package com.comcast.xfinity.sirius.itest

import scalax.file.Path
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.{SiriusResult, RequestHandler}
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
    var latch = new CountDownLatch(expectedTicks)

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

    def resetLatch(newExpectedTicks: Int) {
      latch = new CountDownLatch(newExpectedTicks)
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

  /**
   * Check equality of writeAheadLogs
   */
  def verifyWalsAreEquivalent(expectedSize: Int,
                 first: SiriusLog, rest: SiriusLog*) {

    val wals = Seq(first) ++ rest

    // Timestamps on OrderedEvents are decided at Decision time, so they are not
    // consistent across nodes, they can/should probably be pushed down to be the
    // responsibility of the log
    def extractEvents(wal: SiriusLog) = wal.foldLeft(List[(Long, NonCommutativeSiriusRequest)]()) {
      case (acc, OrderedEvent(seq, _, req)) => Tuple2(seq, req) :: acc
    }

    val walDatas = wals.map(extractEvents(_))

    val areAllSame = walDatas.sliding(2).forall {
      case Seq(lhs, rhs) => lhs == rhs
      case Seq(_) => true
    }

    if (!areAllSame) {
      assert(false, "Logs did not match: " + wals.map(_ + "\n"))
    }

    assert(expectedSize == walDatas.head.size,
      "WALs do not contain all events, " +
        "expected " + expectedSize + ", " +
        "but was " + walDatas.head.size)
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

      assert(waitForTrue(sirius.isOnline, 2000, 500),
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
    sirius1.shutdown(true)
    sirius2.shutdown(true)
    sirius3.shutdown(true)
    tempDir.delete()
  }


  it ("must reach a decision on a series of requests when requests are safely " +
      "set to each node") {
    val reqHandlers = List(reqHandler1, reqHandler2, reqHandler3)

    // Submit requests to node 1
    sirius1.enqueuePut("hello", "world".getBytes)
    sirius1.enqueueDelete("world")
    sirius1.enqueueDelete("yo")

    // Wait for the requests to hit all the handlers
    reqHandlers.foreach(_.await(2, TimeUnit.SECONDS))

    // this is pretty gross, but we don't really have a much better way atm, trevor
    //  has a patch in the works that will make this cool
    Thread.sleep(1000)

    // verify equivalence
    verifyWalsAreEquivalent(3, wal1, wal2, wal3)

    // send an event to node2
    reqHandlers.foreach(_.resetLatch(1))
    sirius2.enqueueDelete("asdf")
    reqHandlers.foreach(_.await(2, TimeUnit.SECONDS))

    // see comment above on thread sleep
    Thread.sleep(500)

    // verify equivalence
    verifyWalsAreEquivalent(4, wal1, wal2, wal3)

    reqHandlers.foreach(_.resetLatch(1))
    sirius3.enqueuePut("xyz", "abc".getBytes)
    reqHandlers.foreach(_.await(2, TimeUnit.SECONDS))

    Thread.sleep(500)

    // verify equivalence
    verifyWalsAreEquivalent(5, wal1, wal2, wal3)
  }

}
