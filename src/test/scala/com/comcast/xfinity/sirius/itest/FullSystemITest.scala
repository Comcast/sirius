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
  class InMemoryLatchedLog(expectedTicks: Int) extends SiriusLog {
    var latch = new CountDownLatch(expectedTicks)

    var entries = List[OrderedEvent]()
    var nextSeq = 1L

    def writeEntry(entry: OrderedEvent) {
      entries = entry :: entries
      nextSeq = entry.sequence + 1
      latch.countDown()
    }

    def createIterator(logRange: LogRange) = {
      throw new IllegalStateException("not implemented")
    }

    def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T = {
      entries.foldRight[T](acc0)(
        (event, acc) => foldFun(acc, event)
      )
    }

    def getNextSeq = nextSeq

    def resetLatch(newExpectedTicks: Int) {
      latch = new CountDownLatch(newExpectedTicks)
    }

    def await(timeout: Long, timeUnit: TimeUnit) {
      latch.await(timeout, timeUnit)
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

  var wal1: InMemoryLatchedLog = _
  var wal2: InMemoryLatchedLog = _
  var wal3: InMemoryLatchedLog = _

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
    wal1 = new InMemoryLatchedLog(3)
    sirius1 = bootNode(reqHandler1, wal1, 42289)

    reqHandler2 = new LatchedRequestHandler(3)
    wal2 = new InMemoryLatchedLog(3)
    sirius2 = bootNode(reqHandler2, wal2, 42290)

    reqHandler3 = new LatchedRequestHandler(3)
    wal3 = new InMemoryLatchedLog(3)
    sirius3 = bootNode(reqHandler3, wal3, 42291)
  }

  after {
    sirius1.shutdown()
    sirius2.shutdown()
    sirius3.shutdown()
    tempDir.delete()
  }

  it ("must reach a decision on a series of requests when requests are safely " +
      "set to each node") {
    val reqHandlers = List(reqHandler1, reqHandler2, reqHandler3)
    val logs = List(wal1, wal2, wal3)

    // Submit requests to node 1, and await ordering
    val result1 = sirius1.enqueuePut("hello", "world".getBytes)
    val result2 = sirius1.enqueueDelete("world")
    val result3 = sirius1.enqueueDelete("yo")

    // Wait for the requests to hit all the logs and request handlers
    logs.foreach(_.await(2, TimeUnit.SECONDS))
    reqHandlers.foreach(_.await(2, TimeUnit.SECONDS))

    // Make sure results are complete and the value checks out
    assert(SiriusResult.none === result1.get(500, TimeUnit.MILLISECONDS))
    assert(SiriusResult.none === result2.get(500, TimeUnit.MILLISECONDS))
    assert(SiriusResult.none === result3.get(500, TimeUnit.MILLISECONDS))

    // verify equivalence
    verifyWalsAreEquivalent(3, wal1, wal2, wal3)

    // same deal, node2
    logs.foreach(_.resetLatch(1))
    reqHandlers.foreach(_.resetLatch(1))
    sirius2.enqueueDelete("asdf")
    logs.foreach(_.await(2, TimeUnit.SECONDS))
    reqHandlers.foreach(_.await(2, TimeUnit.SECONDS))

    // verify equivalence
    verifyWalsAreEquivalent(4, wal1, wal2, wal3)

    // same deal, node 3
    logs.foreach(_.resetLatch(1))
    reqHandlers.foreach(_.resetLatch(1))
    sirius3.enqueuePut("xyz", "abc".getBytes)
    logs.foreach(_.await(2, TimeUnit.SECONDS))
    reqHandlers.foreach(_.await(2, TimeUnit.SECONDS))

    // verify equivalence
    verifyWalsAreEquivalent(5, wal1, wal2, wal3)
  }

}
