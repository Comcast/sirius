package com.comcast.xfinity.sirius.itest

import scalax.file.Path
import com.comcast.xfinity.sirius.{LatchedRequestHandler, TimedTest, NiceTest}
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.{RequestHandler, SiriusConfiguration}
import java.io.File
import com.comcast.xfinity.sirius.api.impl._
import util.Random
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import scala.Some
import scala.Tuple2
import java.util.UUID
import com.comcast.xfinity.sirius.uberstore.UberStore

object FullSystemITest {
  /**
   * get a list of (sequence num, request) from the wals for all their data
   * @param wal log to query
   * @return list of data
   */
  def extractEvents(wal: SiriusLog) = wal.foldLeft(List[(Long, NonCommutativeSiriusRequest)]()) {
    case (acc, OrderedEvent(seq, _, req)) => Tuple2(seq, req) :: acc
  }

  /**
   * Check equality of writeAheadLogs
   */
  def verifyWalsAreEquivalent[A <: SiriusLog](wals: List[A]): Boolean = {
    val walDatas = wals.map(extractEvents(_))

    walDatas.sliding(2).forall {
      case Seq(lhs, rhs) => lhs == rhs
      case Seq(_) => true
    }
  }
  def verifyWalsAreEquivalent(first: SiriusLog, rest: SiriusLog*): Boolean = {
    verifyWalsAreEquivalent(List(first) ++ rest)
  }

  def verifyWalSize(wal: SiriusLog, expectedSize: Long): Boolean = {
    val walData = extractEvents(wal)
    println("Verifying wal size: expected=%s actual=%s".format(expectedSize, walData.size))
    walData.size == expectedSize
  }
}

class FullSystemITest extends NiceTest with TimedTest {

  import FullSystemITest._

  def makeSirius(port: Int,
                 latchTicks: Int = 3,
                 handler: Option[RequestHandler] = None,
                 wal: Option[SiriusLog] = None,
                 chunkSize: Int = 100):
                (SiriusImpl, RequestHandler, SiriusLog) = {

    val finalHandler = handler match {
      case Some(requestHandler) => requestHandler
      case None => new LatchedRequestHandler(latchTicks)
    }
    val finalWal = wal match {
      case Some(siriusLog) => siriusLog
      case None => {
        val uberstoreFile = new File(tempDir, UUID.randomUUID().toString)
        uberstoreFile.mkdir
        UberStore(uberstoreFile.getAbsolutePath)
      }
    }

    val siriusConfig = new SiriusConfiguration()
    siriusConfig.setProp(SiriusConfiguration.HOST, "localhost")
    siriusConfig.setProp(SiriusConfiguration.PORT, port)
    siriusConfig.setProp(SiriusConfiguration.CLUSTER_CONFIG, membershipPath)
    siriusConfig.setProp(SiriusConfiguration.LOG_REQUEST_CHUNK_SIZE, chunkSize)

    val sirius = SiriusFactory.createInstance(
      finalHandler,
      siriusConfig,
      finalWal
    )

    assert(waitForTrue(sirius.isOnline, 2000, 500), "Sirius Node failed to come up within alloted 2 seconds")

    (sirius, finalHandler, finalWal)
  }

  var tempDir: File = _
  var membershipPath: String = _
  var sirii: List[SiriusImpl] = _

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
    membershipPath = membershipFile.getAbsolutePath
  }

  after {
    sirii.foreach(_.shutdown())
    sirii = List()
    tempDir.delete()
  }

  def fireRandomCommands(sirii: List[SiriusImpl], numCommand: Int, startNum: Int = 1) {
    val numSirii = sirii.size
    (startNum to numCommand + startNum - 1).foreach((i) => {
      val siriusNum = Random.nextInt(numSirii)
      sirii(siriusNum).enqueueDelete(i.toString)
      Thread.sleep(10)
    })
  }

  describe("a full sirius implementation") {
    it ("must reach a decision for lots of slots") {
      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290)
      val (sirius3, _, log3) = makeSirius(42291)
      sirii = List(sirius1, sirius2, sirius3)
      val logs = List(log1, log2, log3)

      fireRandomCommands(sirii, numCommands)

      assert(waitForTrue(verifyWalSize(log1, numCommands), 10000, 500),
        "Log did not contain enough events")
      assert(waitForTrue(verifyWalsAreEquivalent(logs), 500, 100),
        "Logs were not equivalent")
    }

    it ("must be able to make progress with a node being down and then catch up") {
      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290)
      sirii = List(sirius1, sirius2)

      fireRandomCommands(sirii, numCommands)

      assert(waitForTrue(verifyWalSize(log1, numCommands), 10000, 500),
        "Pre-new-node log did not contain enough events")
      assert(waitForTrue(verifyWalsAreEquivalent(log1, log2), 500, 100),
        "Pre-new-node logs not equivalent")

      val (sirius3, _, log3) = makeSirius(42291)

      assert(waitForTrue(verifyWalsAreEquivalent(log1, log2, log3), 500, 100),
        "The newly booted node was not equivalent to the rest")

      // so they're all shut down in the end...
      sirii = List(sirius1, sirius2, sirius3)
    }
  }

}
