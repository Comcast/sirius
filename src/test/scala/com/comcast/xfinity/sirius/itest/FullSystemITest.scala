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
                 gapRequestFreqSecs: Int = 30,
                 replicaReproposalWindow: Int = 10):
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
    siriusConfig.setProp(SiriusConfiguration.REPROPOSAL_WINDOW, replicaReproposalWindow)

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

  def fireAndAwait(sirii: List[SiriusImpl], commands: List[String]): List[String] = {
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

  @tailrec
  final def fireAndRetryCommands(sirii: List[SiriusImpl], commands: List[String], retries: Int): List[String] = {
    val failedCommands = fireAndAwait(sirii, commands)
    println("*** %s retries left, need to retry %s commands".format(retries, failedCommands.size))
    if (retries > 0 && !failedCommands.isEmpty) {
      fireAndRetryCommands(sirii, failedCommands, retries - 1)
    } else {
      failedCommands
    }
  }

  def generateCommands(first: Int, last: Int): List[String] = {
    List.range(first, last + 1).map(_.toString)
  }

  describe("a full sirius implementation") {
    it ("must reach a decision for lots of slots") {
      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290)
      val (sirius3, _, log3) = makeSirius(42291)
      sirii = List(sirius1, sirius2, sirius3)
      waitForMembership(sirii, 3)

      val failed = fireAndRetryCommands(sirii, generateCommands(1, numCommands), 4)
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

      val failed = fireAndRetryCommands(sirii, generateCommands(1, numCommands), 4)
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
      val (sirius1, _, log1) = makeSirius(42289, replicaReproposalWindow = 4)
      val (sirius2, _, log2) = makeSirius(42290, replicaReproposalWindow = 4, gapRequestFreqSecs = 5)
      val (sirius3, _, log3) = makeSirius(42291, replicaReproposalWindow = 4, gapRequestFreqSecs = 5)
      sirii = List(sirius1, sirius2, sirius3)
      waitForMembership(sirii, 3)

      val failed = fireAndRetryCommands(sirii, generateCommands(1, numCommands), 4)
      println("No response for %s out of %s".format(failed.size, numCommands))

      assert(waitForTrue(verifyWalSize(log1, numCommands), 30000, 500),
        "Wal 1 did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands))
      assert(waitForTrue(verifyWalSize(log2, numCommands), 30000, 500),
        "Wal 2 did not contain expected number of events (%s out of %s)".format(getWalSize(log2), numCommands))
      assert(waitForTrue(verifyWalSize(log3, numCommands), 30000, 500),
        "Wal 3 did not contain expected number of events (%s out of %s)".format(getWalSize(log3), numCommands))

      assert(waitForTrue(verifyWalsAreEquivalent(List(log1, log2, log3)), 500, 100),
        "Wals were not equivalent")

      path.truncate(0)
      path.append(
        "akka://sirius-42289@localhost:42289/user/sirius\n" +
        "akka://sirius-42290@localhost:42290/user/sirius\n"
      )
      waitForMembership(sirii, 2)

      val nextCommandSet = generateCommands(numCommands + 1, numCommands * 2)
      val nextCommand = nextCommandSet.head
      val nextCommandTail = nextCommandSet.tail

      // use the first command to test... keep going until it finishes.
      // 30 seconds here basically equates to 5 tries (5 sec ask timeout
      // and 1 sec between). this is basically waiting for leader election
      // to occur (if it's necessary).
      var retried = 0
      assert(waitForTrue({
        val failed = fireAndAwait(List(sirius1), List(nextCommand))
        if (!failed.isEmpty) {
          retried += 1
          println("XXX %s retries so far", retried)
        }
        failed.isEmpty
      }, 30000, 1000), "after removing Sirius3 from the cluster, could not get any commands through the system")

      // then fire off the rest
      val failed2 = fireAndRetryCommands(List(sirius1, sirius2), nextCommandTail, 4)
      println("No response for %s out of %s".format(failed2.size, numCommands))

      // nodes of log1 and log2 are running normally
      assert(waitForTrue(verifyWalSize(log1, numCommands * 2), 30000, 500),
        "Wal 1 did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands * 2))
      assert(waitForTrue(verifyWalSize(log2, numCommands * 2), 30000, 500),
        "Wal 2 did not contain expected number of events (%s out of %s)".format(getWalSize(log2), numCommands * 2))

      // log3 is in slave mode, catching up
      assert(waitForTrue(verifyWalSize(log3, numCommands * 2), 30000, 500),
        "Slave did not catch up for all commands (%s out of %s)".format(getWalSize(log3), numCommands * 2))

      assert(waitForTrue(verifyWalsAreEquivalent(List(log1, log2, log3)), 500, 100),
        "Master and slave wals not equivalent")
    }
  }

}
