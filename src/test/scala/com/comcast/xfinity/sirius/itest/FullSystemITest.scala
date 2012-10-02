package com.comcast.xfinity.sirius.itest

import scalax.file.Path
import com.comcast.xfinity.sirius.{LatchedRequestHandler, TimedTest, NiceTest}
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.{RequestHandler, SiriusConfiguration}
import java.io.File
import com.comcast.xfinity.sirius.api.impl._
import bridge.PaxosStateBridge
import membership.CheckClusterConfig
import util.Random
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import scala.Some
import scala.Tuple2
import java.util.UUID
import com.comcast.xfinity.sirius.uberstore.UberStore
import com.comcast.xfinity.sirius.api.impl.SiriusSupervisor.CheckPaxosMembership
import java.util.concurrent.TimeUnit
import annotation.tailrec

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

  def getWalSize(wal: SiriusLog) =
    extractEvents(wal).size

  def verifyWalSize(wal: SiriusLog, expectedSize: Long): Boolean = {
    val walSize = getWalSize(wal)
    println("Verifying wal size: expected=%s actual=%s".format(expectedSize, walSize))
    walSize >= expectedSize
  }
}

class FullSystemITest extends NiceTest with TimedTest {

  import FullSystemITest._

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
      "akka://sirius-system@localhost:42289/user/sirius\n" +
      "akka://sirius-system@localhost:42290/user/sirius\n" +
      "akka://sirius-system@localhost:42291/user/sirius\n"
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
    Thread.sleep(1000)
  }

  def fireAndAwait(sirii: List[SiriusImpl], commands: List[String]) = {
    val numSirii = sirii.size
    val futures = commands.map(
      (i) => {
        Thread.sleep(10)
        (sirii(Random.nextInt(numSirii)).enqueueDelete(i), i)
      }
    )
    futures.foldLeft(List[String]()) {
      case (acc, (future, command)) =>
        try {
          if (false != future.get.hasValue) {
            command :: acc
          } else {
            acc
          }
        } catch {
          case ex: Exception =>
            println("Got exception collecting future for %s: %s".format(command, ex))
            command :: acc
        }
    }.reverse
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
    fireAndRetryCommandsAux(sirii, (first to last).map(_.toString).toList, retries)
  }

  describe("a full sirius implementation") {
    it ("must reach a decision for lots of slots") {
      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290)
      val (sirius3, _, log3) = makeSirius(42291)
      sirii = List(sirius1, sirius2, sirius3)
      waitForMembership(sirii, 3)
      val logs = List(log1, log2, log3)

      val failed = fireAndRetryCommands(sirii, 1, numCommands, 3)
      println("No response for %s out of %s".format(failed.size, numCommands))
      assert(0 === failed.size, "There were failed commands")

      assert(waitForTrue(verifyWalSize(log1, numCommands), 20000, 500),
        "Wal did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands))
      assert(waitForTrue(verifyWalsAreEquivalent(logs), 500, 100),
        "Wals were not equivalent")
    }

    it ("must be able to make progress with a node being down and then catch up") {
      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290)
      sirii = List(sirius1, sirius2)
      waitForMembership(sirii, 3)

      val failed = fireAndRetryCommands(sirii, 1, numCommands, 3)
      println("No response for %s out of %s".format(failed.size, numCommands))

      assert(waitForTrue(verifyWalSize(log1, numCommands), 20000, 500),
        "Wal did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands))
      assert(waitForTrue(verifyWalsAreEquivalent(log1, log2), 500, 100),
        "Wals were not equivalent")

      val (sirius3, _, log3) = makeSirius(42291)
      sirii = List(sirius1, sirius2, sirius3)
      waitForMembership(sirii, 3)

      // TODO: this is gross, we should instead make the catch up interval small in config,
      //       and pass it down.  Just doing this temporarily until we fix the alleged
      //       Bridge/Replica bug
      sirius3.actorSystem.actorFor("/user/sirius/paxos-state-bridge") ! PaxosStateBridge.RequestGaps

      assert(waitForTrue(verifyWalSize(log3, numCommands), 20000, 500),
        "Caught-up wal did not contain expected number of events (%s out of %s)".format(getWalSize(log3), numCommands))
      assert(waitForTrue(verifyWalsAreEquivalent(log1, log2, log3), 2000, 250),
        "Original and caught-up wals were not equivalent")
    }

    it ("must work for master/slave mode") {
      val path = Path.fromString(membershipPath)
      path.delete()
      path.append(
        "akka://sirius-system@localhost:42289/user/sirius\n" +
        "akka://sirius-system@localhost:42290/user/sirius\n" +
        "akka://sirius-system@localhost:42291/user/sirius\n"
      )

      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290, gapRequestFreqSecs = 5)
      val (sirius3, _, log3) = makeSirius(42291)
      sirii = List(sirius1, sirius2, sirius3)
      waitForMembership(sirii, 3)

      val logs = List(log1, log2, log3)

      val failed = fireAndRetryCommands(sirii, 1, numCommands, 3)
      println("No response for %s out of %s".format(failed.size, numCommands))

      // log1 and log2 are working together
      assert(waitForTrue(verifyWalSize(log1, numCommands), 20000, 500),
        "Wal did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands))
      assert(waitForTrue(verifyWalsAreEquivalent(logs), 500, 100),
        "Wals were not equivalent")

      // ok, now let's take 2 out of the cluster...
      // needs to be 1 or 2, since 3 is (probably) the leader and we don't want to sit here
      //   and wait for leader election.
      // XXX "probably" above, arg

      path.delete()
      path.append(
        "akka://sirius-system@localhost:42289/user/sirius\n" +
        "akka://sirius-system@localhost:42291/user/sirius\n"
      )
      waitForMembership(sirii, 2)

      val failed2 = fireAndRetryCommands(List(sirius1, sirius3), numCommands + 1, numCommands * 2, 3)
      println("No response for %s out of %s".format(failed2.size, numCommands))

      // log1 and log3 are working together
      assert(waitForTrue(verifyWalSize(log1, numCommands * 2), 20000, 500),
        "Wal did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands * 2))

      // log2 is in slave mode, catching up
      assert(waitForTrue(verifyWalSize(log2, numCommands * 2), 20000, 500),
        "Slave did not catch up for all commands (%s out of %s)".format(getWalSize(log2), numCommands * 2))

      assert(waitForTrue(verifyWalsAreEquivalent(logs), 500, 100),
        "Master and slave wals not equivalent")
    }
  }

}
