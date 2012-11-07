package com.comcast.xfinity.sirius.itest

import scalax.file.Path
import com.comcast.xfinity.sirius.{LatchedRequestHandler, TimedTest, NiceTest}
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import java.io.File
import com.comcast.xfinity.sirius.api.impl._
import membership.CheckClusterConfig
import util.Random
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import scala.Some
import scala.Tuple2
import java.util.UUID
import com.comcast.xfinity.sirius.uberstore.UberStore
import com.comcast.xfinity.sirius.api.impl.SiriusSupervisor.CheckPaxosMembership
import annotation.tailrec
import com.comcast.xfinity.sirius.api.{SiriusResult, RequestHandler, SiriusConfiguration}
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.SeekLeadership

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

  def getWalSize(wal: SiriusLog) = extractEvents(wal).size

  def verifyWalSize(wal: SiriusLog, expectedSize: Long): Boolean = {
    val walSize = getWalSize(wal)
    println("Verifying wal size: expected=%s actual=%s".format(expectedSize, walSize))
    walSize >= expectedSize
  }
}

class FullSystemITest extends NiceTest with TimedTest {

  import FullSystemITest._

  // the system name this sirius created on will be sirius-$port
  // so that we can better get an idea of what's going on in the
  // logs
  def makeSirius(port: Int,
                 latchTicks: Int = 3,
                 handler: Option[RequestHandler] = None,
                 wal: Option[SiriusLog] = None,
                 chunkSize: Int = 100,
                 gapRequestFreqSecs: Int = 30):
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
    siriusConfig.setProp(SiriusConfiguration.AKKA_SYSTEM_NAME, "sirius-%d".format(port))
    siriusConfig.setProp(SiriusConfiguration.CLUSTER_CONFIG, membershipPath)
    siriusConfig.setProp(SiriusConfiguration.LOG_REQUEST_CHUNK_SIZE, chunkSize)
    siriusConfig.setProp(SiriusConfiguration.LOG_REQUEST_FREQ_SECS, gapRequestFreqSecs)
    siriusConfig.setProp(SiriusConfiguration.PAXOS_MEMBERSHIP_CHECK_INTERVAL, 0.1)

    val sirius = SiriusFactory.createInstance(
      finalHandler,
      siriusConfig,
      finalWal
    )

    assert(waitForTrue(sirius.isOnline, 2000, 500), "Failed while waiting for sirius to boot")

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
    membershipPath = membershipFile.getAbsolutePath
    Path.fromString(membershipPath).append(
      "akka://sirius-42289@localhost:42289/user/sirius\n" +
      "akka://sirius-42290@localhost:42290/user/sirius\n" +
      "akka://sirius-42291@localhost:42291/user/sirius\n"
    )
  }

  after {
    sirii.foreach(_.shutdown())
    sirii = List()
    tempDir.delete()
  }

  def waitForMembership(sirii: List[SiriusImpl], membersExpected: Int) {
    sirii.foreach(_.supervisor ! CheckClusterConfig)
    assert(waitForTrue(sirii.forall(_.getMembership.get.size == membersExpected), 5000, 500),
      "Membership did not reach expected size")
    sirii.foreach(_.supervisor ! CheckPaxosMembership)
    Thread.sleep(2000)
  }

  def fireAndAwait(sirii: List[SiriusImpl], commands: List[String]) = {
    val numSirii = sirii.size
    val futuresAndCommands = commands.map(
      (i) => {
        Thread.sleep(10)
        (sirii(Random.nextInt(numSirii)).enqueueDelete(i), i)
      }
    )
    val failedFuturesAndCommands = futuresAndCommands.filterNot (
      (futureAndCommand) =>
        try {
          SiriusResult.none == futureAndCommand._1.get
        } catch {
          case ex: Exception =>
            println("Future retrieval failed: " + ex)
            false
        }
    )

    failedFuturesAndCommands.map(_._2)
  }

  def fireAndRetryCommands(sirii: List[SiriusImpl], first: Int, last: Int, retries: Int) = {
    @tailrec
    def fireAndRetryCommandsAux(sirii: List[SiriusImpl], commands: List[String], retries: Int): List[String] = {
      val failedCommands = fireAndAwait(sirii, commands)
      println("*** %s retries left, need to retry %s commands".format(retries, failedCommands.size))
      if (retries > 0 && !failedCommands.isEmpty) {
        fireAndRetryCommandsAux(sirii, failedCommands, retries - 1)
      } else {
        failedCommands
      }
    }
    fireAndRetryCommandsAux(sirii, List.range(first, last + 1).map(_.toString), retries)
  }

  describe("a full sirius implementation") {
    it ("must reach a decision for lots of slots") {
      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290)
      val (sirius3, _, log3) = makeSirius(42291)
      sirii = List(sirius1, sirius2, sirius3)
      waitForMembership(sirii, 3)

      val failed = fireAndRetryCommands(sirii, 1, numCommands, 4)
      println("No response for %s out of %s".format(failed.size, numCommands))
      assert(0 === failed.size, "There were failed commands")

      assert(waitForTrue(verifyWalSize(log1, numCommands), 30000, 500),
        "Wal 1 did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands))
      assert(waitForTrue(verifyWalSize(log2, numCommands), 30000, 500),
        "Wal 2 did not contain expected number of events (%s out of %s)".format(getWalSize(log2), numCommands))
      assert(waitForTrue(verifyWalSize(log3, numCommands), 30000, 500),
        "Wal 3 did not contain expected number of events (%s out of %s)".format(getWalSize(log3), numCommands))

      assert(waitForTrue(verifyWalsAreEquivalent(List(log1, log2, log3)), 500, 100),
        "Wals were not equivalent")
    }

    it ("must be able to make progress with a node being down and then catch up") {
      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290)
      sirii = List(sirius1, sirius2)
      waitForMembership(sirii, 3)

      val failed = fireAndRetryCommands(sirii, 1, numCommands, 4)
      println("No response for %s out of %s".format(failed.size, numCommands))

      assert(waitForTrue(verifyWalSize(log1, numCommands), 30000, 500),
        "Wal 1 did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands))
      assert(waitForTrue(verifyWalSize(log2, numCommands), 30000, 500),
        "Wal 2 did not contain expected number of events (%s out of %s)".format(getWalSize(log2), numCommands))

      assert(waitForTrue(verifyWalsAreEquivalent(List(log1, log2)), 500, 100),
        "Wals were not equivalent")

      val (sirius3, _, log3) = makeSirius(42291, gapRequestFreqSecs = 3)
      sirii = List(sirius1, sirius2, sirius3)
      waitForMembership(sirii, 3)

      assert(waitForTrue(verifyWalSize(log3, numCommands), 30000, 500),
        "Caught-up wal did not contain expected number of events (%s out of %s)".format(getWalSize(log3), numCommands))
      assert(waitForTrue(verifyWalsAreEquivalent(List(log1, log2, log3)), 2000, 250),
        "Original and caught-up wals were not equivalent")
    }

    it ("must work for master/slave mode") {
      val path = Path.fromString(membershipPath)
      path.delete()
      path.append(
        "akka://sirius-42289@localhost:42289/user/sirius\n" +
        "akka://sirius-42290@localhost:42290/user/sirius\n" +
        "akka://sirius-42291@localhost:42291/user/sirius\n"
      )

      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290, gapRequestFreqSecs = 5)
      val (sirius3, _, log3) = makeSirius(42291)
      sirii = List(sirius1, sirius2, sirius3)
      waitForMembership(sirii, 3)

      val failed = fireAndRetryCommands(sirii, 1, numCommands, 4)
      println("No response for %s out of %s".format(failed.size, numCommands))

      assert(waitForTrue(verifyWalSize(log1, numCommands), 30000, 500),
        "Wal 1 did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands))
      assert(waitForTrue(verifyWalSize(log2, numCommands), 30000, 500),
        "Wal 2 did not contain expected number of events (%s out of %s)".format(getWalSize(log2), numCommands))
      assert(waitForTrue(verifyWalSize(log3, numCommands), 30000, 500),
        "Wal 3 did not contain expected number of events (%s out of %s)".format(getWalSize(log3), numCommands))

      assert(waitForTrue(verifyWalsAreEquivalent(List(log1, log2, log3)), 500, 100),
        "Wals were not equivalent")

      // ask sirius1 to assume leadership, then remove sirius2 from the cluster
      val leader1 = sirius1.actorSystem.actorFor("/user/sirius/paxos/leader")
      leader1 ! SeekLeadership
      Thread.sleep(2000)

      path.truncate(0)
      path.append(
        "akka://sirius-42289@localhost:42289/user/sirius\n" +
        "akka://sirius-42291@localhost:42291/user/sirius\n"
      )
      waitForMembership(sirii, 2)

      val failed2 = fireAndRetryCommands(List(sirius1, sirius3), numCommands + 1, numCommands * 2, 4)
      println("No response for %s out of %s".format(failed2.size, numCommands))

      // nodes of log1 and log3 are running normally
      assert(waitForTrue(verifyWalSize(log1, numCommands * 2), 30000, 500),
        "Wal 1 did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands * 2))
      assert(waitForTrue(verifyWalSize(log3, numCommands * 2), 30000, 500),
        "Wal 3 did not contain expected number of events (%s out of %s)".format(getWalSize(log3), numCommands * 2))

      // log2 is in slave mode, catching up
      assert(waitForTrue(verifyWalSize(log2, numCommands * 2), 30000, 500),
        "Slave did not catch up for all commands (%s out of %s)".format(getWalSize(log2), numCommands * 2))

      assert(waitForTrue(verifyWalsAreEquivalent(List(log1, log2, log3)), 500, 100),
        "Master and slave wals not equivalent")
    }
  }

}
