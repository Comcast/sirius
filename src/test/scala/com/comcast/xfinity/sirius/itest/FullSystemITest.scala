/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.comcast.xfinity.sirius.itest

import scalax.file.Path
import com.comcast.xfinity.sirius.{LatchedRequestHandler, TimedTest, NiceTest}
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import java.io.File
import com.comcast.xfinity.sirius.api.impl._
import util.Random
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import scala.Some
import scala.Tuple2
import java.util.UUID
import com.comcast.xfinity.sirius.uberstore.UberStore
import com.comcast.xfinity.sirius.api.impl.SiriusSupervisor.CheckPaxosMembership
import annotation.tailrec
import com.comcast.xfinity.sirius.api.{SiriusResult, RequestHandler, SiriusConfiguration}
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor.CheckClusterConfig
import org.slf4j.LoggerFactory
import com.comcast.xfinity.sirius.uberstore.segmented.SegmentedUberStore

object FullSystemITest {

  def getProtocol(sslEnabled: Boolean): String =
    sslEnabled match {
      case true => "akka.ssl.tcp"
      case false => "akka.tcp"
    }

  val logger = LoggerFactory.getLogger(FullSystemITest.getClass)
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
    logger.debug("Verifying wal size: expected=%s actual=%s".format(expectedSize, walSize))
    walSize >= expectedSize
  }
}

class FullSystemITest extends NiceTest with TimedTest {

  import FullSystemITest._

  val logger = LoggerFactory.getLogger(classOf[FullSystemITest])

  // the system name this sirius created on will be sirius-$port
  // so that we can better get an idea of what's going on in the
  // logs
  def makeSirius(port: Int,
                 latchTicks: Int = 3,
                 handler: Option[RequestHandler] = None,
                 wal: Option[SiriusLog] = None,
                 chunkSize: Int = 100,
                 gapRequestFreqSecs: Int = 5,
                 replicaReproposalWindow: Int = 10,
                 sslEnabled: Boolean = false,
                 maxWindowSize: Int = 1000,
                 membershipPath: String = new File(tempDir, "membership").getAbsolutePath):
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
    siriusConfig.setProp(SiriusConfiguration.ENABLE_SSL, sslEnabled)
    siriusConfig.setProp(SiriusConfiguration.AKKA_SYSTEM_NAME, "sirius-%d".format(port))
    siriusConfig.setProp(SiriusConfiguration.CLUSTER_CONFIG, membershipPath)
    siriusConfig.setProp(SiriusConfiguration.LOG_REQUEST_CHUNK_SIZE, chunkSize)
    siriusConfig.setProp(SiriusConfiguration.LOG_REQUEST_FREQ_SECS, gapRequestFreqSecs)
    siriusConfig.setProp(SiriusConfiguration.PAXOS_MEMBERSHIP_CHECK_INTERVAL, 0.1)
    siriusConfig.setProp(SiriusConfiguration.REPROPOSAL_WINDOW, replicaReproposalWindow)
    siriusConfig.setProp(SiriusConfiguration.CATCHUP_MAX_WINDOW_SIZE, maxWindowSize)

    if (sslEnabled) {
      siriusConfig.setProp(SiriusConfiguration.AKKA_EXTERN_CONFIG, "src/test/resources/sirius-akka-base.conf")
      siriusConfig.setProp(SiriusConfiguration.KEY_STORE_LOCATION, "src/test/resources/keystore")
      siriusConfig.setProp(SiriusConfiguration.TRUST_STORE_LOCATION, "src/test/resources/truststore")
      siriusConfig.setProp(SiriusConfiguration.KEY_STORE_PASSWORD, "password")
      siriusConfig.setProp(SiriusConfiguration.KEY_PASSWORD, "password")
      siriusConfig.setProp(SiriusConfiguration.TRUST_STORE_PASSWORD, "password")
    }

    val sirius = SiriusFactory.createInstance(
      finalHandler,
      siriusConfig,
      finalWal
    )

    assert(waitForTrue(sirius.isOnline, 2000, 500), "Failed while waiting for sirius to boot")

    (sirius, finalHandler, finalWal)
  }

  var tempDir: File = _
  var sirii: List[SiriusImpl] = List()

  before {
    val tempDirName = "%s/sirius-fulltest-%s".format(
      System.getProperty("java.io.tmpdir"),
      System.currentTimeMillis()
    )
    tempDir = new File(tempDirName)
    tempDir.mkdirs()

    writeClusterConfig(List()) // write blank cluster config to start
  }

  after {
    sirii.foreach(_.shutdown())
    sirii = List()
    Path(tempDir).deleteRecursively()
  }

  def writeClusterConfig(ports: List[Int], sslEnabled: Boolean = false) {
    val protocol = FullSystemITest.getProtocol(sslEnabled)
    val config = ports.map(port => "%s://sirius-%s@localhost:%s/user/sirius\n".format(protocol, port, port))
                      .foldLeft("")(_ + _)

    val membershipFile = new File(tempDir, "membership")
    val membershipPath = membershipFile.getAbsolutePath
    Path.fromString(membershipPath).delete(force = true)
    Path.fromString(membershipPath).append(config)
  }

  def waitForMembership(sirii: List[SiriusImpl], membersExpected: Int) {
    assert(waitForTrue({
      sirii.foreach(_.supervisor ! CheckClusterConfig)
      sirii.forall(_.getMembership.get.values.flatten.size == membersExpected)
    }, 10000, 1500),
      "Membership did not reach expected size: expected %s, got %s"
        .format(membersExpected, sirii.map(_.getMembership.get.values.flatten.size)))
    sirii.foreach(_.supervisor ! CheckPaxosMembership)
    Thread.sleep(1000)
    // TODO assert that each node turns on its Paxos
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
            logger.debug("Future retrieval failed for command %s".format(futureAndCommand._2))
            false
        }
    )

    failedFuturesAndCommands.map(_._2)
  }

  @tailrec
  final def fireAndRetryCommands(sirii: List[SiriusImpl], commands: List[String], retries: Int): List[String] = {
    val failedCommands = fireAndAwait(sirii, commands)
    logger.debug("*** %s retries left, need to retry %s commands".format(retries, failedCommands.size))
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
    it("must reach a decision for lots of slots") {
      writeClusterConfig(List(42289, 42290, 42291))
      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290)
      val (sirius3, _, log3) = makeSirius(42291)
      sirii = List(sirius1, sirius2, sirius3)
      waitForMembership(sirii, 3)

      val failed = fireAndRetryCommands(sirii, generateCommands(1, numCommands), 4)
      logger.debug("No response for %s out of %s".format(failed.size, numCommands))
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

    it("must be able to make progress with a node being down and then catch up") {
      writeClusterConfig(List(42289, 42290, 42291))
      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289)
      val (sirius2, _, log2) = makeSirius(42290)
      sirii = List(sirius1, sirius2)
      waitForMembership(sirii, 2)

      val failed = fireAndRetryCommands(sirii, generateCommands(1, numCommands), 4)
      logger.debug("No response for %s out of %s".format(failed.size, numCommands))

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

    it("must work for master/slave mode") {
      writeClusterConfig(List(42289, 42290, 42291))
      val numCommands = 50
      val (sirius1, _, log1) = makeSirius(42289, replicaReproposalWindow = 4)
      val (sirius2, _, log2) = makeSirius(42290, replicaReproposalWindow = 4)
      val (sirius3, _, log3) = makeSirius(42291, replicaReproposalWindow = 4)
      sirii = List(sirius1, sirius2, sirius3)
      waitForMembership(sirii, 3)

      val failed = fireAndRetryCommands(sirii, generateCommands(1, numCommands), 4)
      logger.debug("No response for %s out of %s".format(failed.size, numCommands))

      assert(waitForTrue(verifyWalSize(log1, numCommands), 30000, 500),
        "Wal 1 did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands))
      assert(waitForTrue(verifyWalSize(log2, numCommands), 30000, 500),
        "Wal 2 did not contain expected number of events (%s out of %s)".format(getWalSize(log2), numCommands))
      assert(waitForTrue(verifyWalSize(log3, numCommands), 30000, 500),
        "Wal 3 did not contain expected number of events (%s out of %s)".format(getWalSize(log3), numCommands))

      assert(waitForTrue(verifyWalsAreEquivalent(List(log1, log2, log3)), 500, 100),
        "Wals were not equivalent")

      writeClusterConfig(List(42289, 42290))
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
          logger.debug("%s retries so far".format(retried))
        }
        failed.isEmpty
      }, 30000, 1000), "after removing Sirius3 from the cluster, could not get any commands through the system")

      // then fire off the rest
      val failed2 = fireAndRetryCommands(List(sirius1, sirius2), nextCommandTail, 4)
      logger.debug("No response for %s out of %s".format(failed2.size, numCommands))

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

  it("should fail to start if a segmented WAL is specified but the LOG_VERSION_ID param does not match") {
    SegmentedUberStore.init(tempDir.getAbsolutePath)

    intercept[IllegalStateException] {
      UberStore(tempDir.getAbsolutePath)
    }
  }

  it("should keep its membership updated properly when remote nodes go down") {
    writeClusterConfig(List(42289, 42290, 42291))
    val (sirius1, _, _) = makeSirius(42289)
    val (sirius2, _, _) = makeSirius(42290)
    val (sirius3, _, _) = makeSirius(42291)
    sirii = List(sirius1, sirius2, sirius3)
    waitForMembership(sirii, 3)

    sirius3.shutdown()
    sirii = List(sirius1, sirius2)

    waitForMembership(sirii, 2)
  }

  it("should be able to make progress with SSL turned on") {
    writeClusterConfig(List(42289, 42290, 42291), sslEnabled = true)
    val (sirius1, _, log1) = makeSirius(42289, sslEnabled = true)
    val (sirius2, _, log2) = makeSirius(42290, sslEnabled = true)
    val (sirius3, _, log3) = makeSirius(42291, sslEnabled = true)

    sirii = List(sirius1, sirius2, sirius3)
    waitForMembership(sirii, 3)

    val numCommands = 10
    val failed = fireAndRetryCommands(sirii, generateCommands(1, numCommands), 4)
    logger.debug("No response for %s out of %s".format(failed.size, numCommands))
    assert(0 === failed.size, "There were failed commands")

    assert(waitForTrue(verifyWalSize(log1, numCommands), 30000, 500),
      "Wal 1 did not contain expected number of events (%s out of %s)".format(getWalSize(log1), numCommands))
    assert(waitForTrue(verifyWalSize(log2, numCommands), 30000, 500),
      "Wal 2 did not contain expected number of events (%s out of %s)".format(getWalSize(log2), numCommands))
    assert(waitForTrue(verifyWalSize(log3, numCommands), 30000, 500),
      "Wal 3 did not contain expected number of events (%s out of %s)".format(getWalSize(log3), numCommands))

    assert(waitForTrue(verifyWalsAreEquivalent(List(log1, log2, log3)), 10000, 100),
      "Wals were not equivalent")
  }

  def createSegmentedLog(name: String, maxEvents: Option[Long] = None): SegmentedUberStore = {
    val config = new SiriusConfiguration()

    maxEvents foreach {
      config.setProp(SiriusConfiguration.LOG_EVENTS_PER_SEGMENT, _)
    }
    SegmentedUberStore.init(new File(tempDir, name).getAbsolutePath)
    SegmentedUberStore(new File(tempDir, name).getAbsolutePath, config)
  }

  it("should be able to catchup even after the next item has been compacted away") {
    val log1 = createSegmentedLog("1", maxEvents = Some(4L))
    log1.writeEntry(OrderedEvent(1, 0, Delete("1")))
    log1.writeEntry(OrderedEvent(2, 0, Delete("2")))
    log1.writeEntry(OrderedEvent(3, 0, Delete("3")))
    log1.writeEntry(OrderedEvent(4, 0, Delete("4")))

    log1.writeEntry(OrderedEvent(5, 0, Delete("3")))
    log1.writeEntry(OrderedEvent(6, 0, Delete("4")))
    log1.writeEntry(OrderedEvent(7, 0, Delete("5")))
    log1.writeEntry(OrderedEvent(8, 0, Delete("6")))

    val log2 = createSegmentedLog("2", maxEvents = Some(4L))
    log2.writeEntry(OrderedEvent(1, 0, Delete("1")))
    log2.writeEntry(OrderedEvent(2, 0, Delete("2")))

    assert(verifyWalSize(log1, 8))
    assert(verifyWalSize(log2, 2))

    log1.compact()

    assert(verifyWalSize(log1, 6))

    writeClusterConfig(List(42289, 42290))
    val (sirius1, _, _) = makeSirius(42289, wal = Some(log1))
    val (sirius2, _, _) = makeSirius(42290, wal = Some(log2))
    sirii = List(sirius1, sirius2)
    waitForMembership(sirii, 2)

    assert(waitForTrue(verifyWalSize(log2, 6), 30000, 500), "Follower did not catch up")
  }

  it("should be able to catchup even after the next item (and its entire segment) has been compacted away") {
    val log1 = createSegmentedLog("1", maxEvents = Some(4L))
    log1.writeEntry(OrderedEvent(1, 0, Delete("1")))
    log1.writeEntry(OrderedEvent(2, 0, Delete("2")))
    log1.writeEntry(OrderedEvent(3, 0, Delete("3")))
    log1.writeEntry(OrderedEvent(4, 0, Delete("4")))

    // entire second segment is compacted away
    log1.writeEntry(OrderedEvent(5, 0, Delete("5")))
    log1.writeEntry(OrderedEvent(6, 0, Delete("6")))
    log1.writeEntry(OrderedEvent(7, 0, Delete("7")))
    log1.writeEntry(OrderedEvent(8, 0, Delete("8")))

    log1.writeEntry(OrderedEvent(9, 0, Delete("5")))
    log1.writeEntry(OrderedEvent(10, 0, Delete("6")))
    log1.writeEntry(OrderedEvent(11, 0, Delete("7")))
    log1.writeEntry(OrderedEvent(12, 0, Delete("8")))

    log1.writeEntry(OrderedEvent(13, 0, Delete("9")))

    val log2 = createSegmentedLog("2", maxEvents = Some(4L))
    log2.writeEntry(OrderedEvent(1, 0, Delete("1")))
    log2.writeEntry(OrderedEvent(2, 0, Delete("2")))
    log2.writeEntry(OrderedEvent(3, 0, Delete("3")))
    log2.writeEntry(OrderedEvent(4, 0, Delete("4")))

    assert(verifyWalSize(log1, 11))
    assert(verifyWalSize(log2, 4))

    log1.compact()

    assert(verifyWalSize(log1, 9))

    writeClusterConfig(List(42289, 42290))
    val (sirius1, _, _) = makeSirius(42289, wal = Some(log1), maxWindowSize = 2)
    val (sirius2, _, _) = makeSirius(42290, wal = Some(log2), maxWindowSize = 2)
    sirii = List(sirius1, sirius2)
    waitForMembership(sirii, 2)

    assert(waitForTrue(verifyWalSize(log2, 9), 30000, 500), "Follower did not catch up")
  }
}
