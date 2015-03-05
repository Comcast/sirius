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
package com.comcast.xfinity.sirius.tool

import java.io._
import scala.util.matching.Regex
import com.comcast.xfinity.sirius.api.impl._
import com.comcast.xfinity.sirius.tool.format.OrderedEventFormatter
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.uberstore.segmented.SegmentedUberStore
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.uberstore.{UberTool, UberStore}
import scala.concurrent.{Await, Future}
import scala.collection.mutable.Map
import akka.actor.{ActorSystem, Props, Actor}
import akka.routing.RoundRobinRouter
import akka.pattern.ask
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.util.Timeout
import java.net.{HttpURLConnection, URL}
import scala._
import java.lang.String
import scala.Left
import com.comcast.xfinity.sirius.api.impl.Put
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import scala.Right
import scala.Console
import com.comcast.xfinity.sirius.api.impl.Delete
import scala.collection.mutable.WrappedArray
import scala.collection.mutable.Set
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

/**
 * Object meant to be invoked as a main class from the terminal.  Provides some
 * simple write ahead log operations.
 *
 * See usage for usage
 */
object WalTool {

  private def printUsage() {
    Console.err.println("Usage:")
    Console.err.println("   compact [two-pass] <inWalDir> <outWalDir>")
    Console.err.println("       Compact UberStore in inWalDir into new UberStore in outWalDir")
    Console.err.println("       If two-pass is provided then the more memory efficient two pass")
    Console.err.println("       compaction algorithm will be used.  All deletes older than 1 week")
    Console.err.println("       are fully removed from the resultant log. Will error if called")
    Console.err.println("       against a SegmentedUberStore.")
    Console.err.println()
    Console.err.println("   compact-segmented <walDir>")
    Console.err.println("       Compact SegmentedUberStore in walDir. Will error if called against")
    Console.err.println("       a non-segmented UberStore.")
    Console.err.println()
    Console.err.println("   tail [-n number] <walDir>")
    Console.err.println("       Show last 20 sequence numbers in wal.")
    Console.err.println("       -n: number of lines to show (e.g., -n 10, -n 50), defaults to 20")
    Console.err.println()
    Console.err.println("   range <begin> <end> <walDir>")
    Console.err.println("       Show all entries in begin to end range, inclusive")
    Console.err.println()
    Console.err.println("   key-filter <regexp> <inWalDir> <outWalDir>")
    Console.err.println("       Write all OrderedEvents in UberStore in inWalDir having a key")
    Console.err.println("       matching regexp to new UberStore in outWalDir")
    Console.err.println()
    Console.err.println("   key-filter-not <regexp> <inWalDir> <outWalDir>")
    Console.err.println("       Same as key-filter, except the resulting UberStore contains all")
    Console.err.println("       OrderedEvents not matching regexp")
    Console.err.println()
    Console.err.println("   key-filter-deletes-not-older-than <earliestMillis> <inWalDir> <outWalDir>")
    Console.err.println("       Write all OrderedEvents in UberStore to a new UberStore in outWalDir ")
    Console.err.println("       while filtering Deletes that are older than the specified timestamp ")
    Console.err.println()
    Console.err.println("   key-filter-puts-not-older-than <regexp> <earliestMillis> <inWalDir> <outWalDir>")
    Console.err.println("       Write all OrderedEvents in UberStore in inWalDir to a new UberStore in outWalDir")
    Console.err.println("       while filtering Puts that match the regexp and are older than the specified timestamp")
    Console.err.println()
    Console.err.println("   key-list <inWalDir> <outFile>")
    Console.err.println("       Write all keys from wal in outFile")
    Console.err.println()
    Console.err.println("   key-list-filter <regexp> <inWalDir> <outFile>")
    Console.err.println("       write all keys that match regexp to outFile")
    Console.err.println()
    Console.err.println("   key-list-filter-puts-older-than <regexp> <earliestMillis> <inWalDir> <outFile>")
    Console.err.println("       Write all Put keys that match regexp and are older than the specified timestamp")
    Console.err.println("       to outFile")
    Console.err.println()
    Console.err.println("   replay <inWalDir> <host> <concurrency>")
    Console.err.println("       For each OrderedEvent will issue an http request")
    Console.err.println()
    Console.err.println("   convert-to-segmented <inWalDir> <outWalDir> <segmentSize>")
    Console.err.println("       Convert a LegacyUberStore into a SegmentedUberStore.")
    Console.err.println("       SegmentSize is the number of entries to write per segment, and is required.")
    Console.err.println()
    Console.err.println("   convert-to-legacy <inWalDir> <outWalDir>")
    Console.err.println("       Convert a SegmentedUberStore into a LegacyUberStore")
    Console.err.println()
    Console.err.println("   is-segmented <walDir>")
    Console.err.println("       Returns 0 if the specified walDir is a segmented UberStore, 1 otherwise")
    Console.err.println()
    Console.err.println("   is-legacy <walDir>")
    Console.err.println("       Returns 0 if the specified walDir is a legacy UberStore, 1 otherwise")
    Console.err.println()
    Console.err.println("   init-segmented <walDir>")
    Console.err.println("       Initialize a new SegmentedUberStore at walDir. Creates the necessary")
    Console.err.println("       space and marks it appropriately.")
  }

  def main(args: Array[String]) {
    args match {
      case Array("compact", inWalDirName, outWalDirName) =>
        compact(inWalDirName, outWalDirName, false)
      case Array("compact", "two-pass", inWalDirName, outWalDirName) =>
        compact(inWalDirName, outWalDirName, true)

      case Array("compact-segmented", walDirName) =>
        compactSegmented(walDirName)

      case Array("tail", walDir) =>
        tailUber(walDir)
      case Array("tail", "-n", number, walDir) =>
        tailUber(walDir, number.toInt)

      case Array("range", begin, end, walDir) =>
        printSeq(siriusLog(walDir), begin.toLong, end.toLong)

      case Array("key-filter", regexpStr, inWal, outWal) =>
        val regexp = regexpStr.r
        val filterFun: OrderedEvent => Boolean = keyMatch(regexp, _)
        filter(inWal, outWal, filterFun)

      case Array("key-filter-deletes-not-older-than", earlistTimestamp, inWal, outFile) =>
        filterDeletesOlderThan(inWal, outFile, earlistTimestamp.toLong)

      case Array("key-filter-puts-not-older-than", regexpStr, earliestTimestamp, inWal, outWal) =>
        val regexp = regexpStr.r
        val filterFun: OrderedEvent => Boolean = !keyMatchPutsOlderThan(regexp, earliestTimestamp.toLong, _)
        filter(inWal, outWal, filterFun)

      case Array("key-filter-not", regexpStr, inWal, outWal) =>
        val regexp = regexpStr.r
        val filterFun: OrderedEvent => Boolean = !keyMatch(regexp, _)
        filter(inWal, outWal, filterFun)

      case Array("key-list", inWal, outFile) =>
        keyList(inWal,outFile)

      case Array("key-list-filter", regexpStr, inWal, outFile) =>
        val regexp = regexpStr.r
        val filterFun: OrderedEvent => Boolean = keyMatch(regexp, _)
        keyListFilter(inWal, outFile, filterFun)

      case Array("key-list-filter-puts-older-than", regexpStr, earliestTimestamp, inWal, outFile) =>
        val regexp = regexpStr.r
        val filterFun: OrderedEvent => Boolean = keyMatchPutsOlderThan(regexp, earliestTimestamp.toLong, _)
        keyListFilter(inWal, outFile, filterFun)

      case Array("replay", inWal, host, concurrency) =>
        replay(inWal, host, concurrency.toInt)

      case Array("convert-to-segmented", inWal, outWal, segmentSize) =>
        convertToSegmented(inWal, outWal, segmentSize.toLong)

      case Array("convert-to-legacy", inWal, outWal) =>
        convertToLegacy(inWal, outWal)

      case Array("is-segmented", wal) =>
        if (!UberTool.isSegmented(wal)) {
          sys.exit(1)
        }

      case Array("is-legacy", wal) =>
        if (!UberTool.isLegacy(wal)) {
          sys.exit(1)
        }

      case Array("init-segmented", walDir) =>
        initSegmented(walDir)

      case _ => printUsage()
    }
    sys.exit(0)
  }

  /**
   * Compact SegmentedUberStore
   *
   * @param walDirName directory of the segmented uberstore
   */
  private def compactSegmented(walDirName: String) {
    val uberstore = SegmentedUberStore(walDirName)
    uberstore.compact()
  }

  /**
   * Compact an UberStore
   *
   * @param inWalDirName directory specifying input UberStore
   * @param outWalDirName directory specifying output UberStore
   * @param twoPass true to use slower, more memory efficient twoPass algorithm,
   *          false to use faster, more memory intensive algorithm
   */
  private def compact(inWalDirName: String, outWalDirName: String, twoPass: Boolean) {
    // create dir first because UberStore instantiation can take some time
    createFreshDir(outWalDirName)

    val inWal = UberStore(inWalDirName)
    val outWal = UberStore(outWalDirName)

    doCompact(inWal, outWal, twoPass)
  }

  /**
   * HELPER FUNCTION REALLY REALLY INTERNAL ONLY
   *
   * Compact inWal into outWal using twoPass algorithm if specified
   */
  private def doCompact(inWal: SiriusLog, outWal: SiriusLog, twoPass: Boolean) {
    val cutoff = {
      // unofficial hidden feature, just in case there is some situation we don't
      // want to not compact away old deletes without recompiling
      val maxDeleteAge = System.getProperty("maxDeleteAge", "604800000").toLong
      if (maxDeleteAge <= 0) 0 else (System.currentTimeMillis - maxDeleteAge)
    }

    if (twoPass) {
      UberTool.twoPassCompact(inWal, outWal, cutoff)
    } else {
      UberTool.compact(inWal, outWal, cutoff)
    }
  }

  /**
   * HELPER FUNCTION REALLY REALLY INTERNAL ONLY
   *
   * Create a directory only if it does not exist, throw
   * an Exception otherwise
   */
  private def createFreshDir(dirName: String) {
    val dir = new File(dirName)
    if (dir.exists()) {
      throw new Exception(dirName + " already exists")
    }

    if (!dir.mkdir()) {
      throw new Exception("Failed to create " + dirName)
    }
  }

  /**
   * Tail the binary log, similar to the unix tail tool.
   *
   * @param inDirName location of UberStore
   * @param number number of lines to print, default 20
   */
  private def tailUber(inDirName: String, number: Int = 20) {
    val wal = siriusLog(inDirName)
    val seq = wal.getNextSeq - 1

    printSeq(wal, seq - number, seq)
  }

  /**
   * Internal helper for tailUber, does the actual printing of a range.
   *
   * @param wal uberstore to target
   * @param first first seq to print
   * @param last last seq to print
   */
  private def printSeq(wal: SiriusLog, first: Long, last: Long) {
    wal.foldLeftRange(first, last)(())((_, event) =>
      OrderedEventFormatter.printEvent(event)
    )
  }

  /**
   * Does the key of the OrderedEvent's contains NonCommutativeSiriusRequest match
   * r?
   *
   * @param r Regexp to match
   * @param evt OrderedEvent to check
   */
  private def keyMatch(r: Regex, evt: OrderedEvent): Boolean = evt match {
    case OrderedEvent(_, _, Put(key, _)) => r.findFirstIn(key) != None
    case OrderedEvent(_, _, Delete(key)) => r.findFirstIn(key) != None
    case _ => false
  }

  /**
   * Does the key match r and is a Put and is older than the specified timestamp
   * @param r Regexp to match
   * @param earliestTimestamp timestamp to filter Puts if older than.
   * @param evt OrderedEvent to check
   */
  private def keyMatchPutsOlderThan(r: Regex, earliestTimestamp: Long, evt: OrderedEvent): Boolean = {
    evt match {
      case OrderedEvent(_, timestamp, Put(key, _)) => (r.findFirstIn(key) != None) && (timestamp < earliestTimestamp)
      case _ => false
    }
  }

  private def keyList(inWal:String, outFile:String){
    val wal: SiriusLog = siriusLog(inWal)
    val keySet = Set[WrappedArray[Byte]]()
    wal.foreach(_.request match {
      case (put:Put) => keySet += WrappedArray.make(put.key.getBytes)
      case (delete: Delete) =>  keySet -= WrappedArray.make(delete.key.getBytes)
    })

    val out = new PrintWriter( outFile , "UTF-8")
    try{
      keySet.foreach( (key : WrappedArray[Byte]) => {
        out.println(new String(key.toArray))
      })

    }finally{
      out.close()
    }

   }

  private def keyListFilter(inWal:String, outFile:String, pred: OrderedEvent => Boolean){
    val wal  = siriusLog(inWal)
    val keySet = scala.collection.mutable.Set[WrappedArray[Byte]]()
    wal.foreach( _ match {
      case (evt: OrderedEvent) if pred(evt) => evt.request match {
        case (put:Put) => keySet += WrappedArray.make(put.key.getBytes)
        case (delete: Delete) => keySet -= WrappedArray.make(delete.key.getBytes)
      }
      case _ => //ignore filtered
    })

    val out = new PrintWriter( outFile , "UTF-8")
    try{
      keySet.foreach( (key : WrappedArray[Byte]) => {
        out.println(new String(key.toArray))
      })
    }finally{
      out.close()
    }
  }

  /**
   * Write all entries from UberStore specified by inUberDir to outUberDir (which should not exist)
   * while filtering deletes that are older the specified timestamp.
   *
   * @param earliestTimestamp the earliest timestamp
   * @param inUberDir input UberStore directory
   * @param outUberDir output UberStore directory
   */
  private def filterDeletesOlderThan(inUberDir: String, outUberDir: String, earliestTimestamp: Long) {
    createFreshDir(outUberDir)

    val inWal = siriusLog(inUberDir)

    val outWal = inUberDir match {
      case (dir : String) if UberTool.isLegacy(dir) => UberStore(outUberDir)
      case (dir: String) if UberTool.isSegmented(dir) =>
        SegmentedUberStore.init(outUberDir)
        SegmentedUberStore(outUberDir)
      case _ => throw new IllegalArgumentException(inUberDir + " does not appear to be a valid Uberstore")
    }

    inWal.foreach {
      case evt if filterDeletesOlderThan(evt, earliestTimestamp) != None => outWal.writeEntry(evt)
      case _ =>
    }
  }

  private def filterDeletesOlderThan(evt: OrderedEvent, earliestTimestamp: Long): Option[OrderedEvent] = {
    evt.request match {
      case (put: Put) => Some(evt)
      case (delete: Delete) if evt.timestamp >= earliestTimestamp => Some(evt)
      case _ => None
    }
  }

  /**
   * Write all entries from UberStore specified by inUberDir satisfying
   * pred to outUberDir, which should not exist.
   *
   * @param inUberDir input UberStore directory
   * @param outUberDir output UberStore directory
   * @param pred function taking an OrderedEvent and returning true if
   *          it should be included, false if not
   */
  private def filter(inUberDir: String, outUberDir: String, pred: OrderedEvent => Boolean) {
    createFreshDir(outUberDir)

    val inWal = siriusLog(inUberDir)

    val outWal = inUberDir match {
      case (dir : String) if UberTool.isLegacy(dir) => UberStore(outUberDir)
      case (dir: String) if UberTool.isSegmented(dir) =>
        SegmentedUberStore.init(outUberDir)
        SegmentedUberStore(outUberDir)
      case _ => throw new IllegalArgumentException(inUberDir + " does not appear to be a valid Uberstore")
    }

    filter(inWal, outWal, pred)
  }

  /**
   * Write all entries in inWal which satisfy pred to outWal
   *
   * @param inWal input write ahead log
   * @param outWal output write ahead log
   * @param pred function taking an OrderedEvent and returning true if
   *          it should be included, false if not
   */
  private def filter(inWal: SiriusLog, outWal: SiriusLog, pred: OrderedEvent => Boolean) {
    inWal.foreach {
      case evt if pred(evt) => outWal.writeEntry(evt)
      case _ =>
    }
  }

  /**
   * Convert a Legacy WAL into a Segmented WAL
   *
   * @param inLoc location of input (Legacy) WAL
   * @param outLoc location of output (Segmented) WAL
   * @param segmentSize number of entries per WAL segment
   */
  private def convertToSegmented(inLoc: String, outLoc: String, segmentSize: Long) {
    val config = new SiriusConfiguration
    config.setProp(SiriusConfiguration.LOG_EVENTS_PER_SEGMENT, segmentSize)

    val in = UberStore(inLoc)

    SegmentedUberStore.init(outLoc)
    val out = SegmentedUberStore(outLoc, config)

    in.foreach(out.writeEntry)
  }

  /**
   * Convert a Segmented WAL into a Legacy WAL
   *
   * @param inLoc location of input (Segmented) WAL
   * @param outLoc location of output (Legacy) WAL
   */
  private def convertToLegacy(inLoc: String, outLoc: String) {
    val in = SegmentedUberStore(inLoc)
    val out = UberStore(outLoc)

    in.foreach(out.writeEntry)
  }

  /**
   * Initialize a location for use as a Segmented WAL
   *
   * @param walLoc location of new WAL
   */
  private def initSegmented(walLoc: String) {
    SegmentedUberStore.init(walLoc)
  }

  case class WalAccumulator(totalEvents: Int, futures: List[Future[Result]])
  case class Send(evt: OrderedEvent, host: String)
  case class Result(duration: Int, statusCode: Int)

  /**
   * Generates PUT/DELETE traffic from a WAL
   *
   * @param inWalDir dir for write ahead log
   * @param host where load should be directed
   * @param concurrency approximate concurrency desired
   */
  // TODO pull parts of this out into UberTool
  private def replay(inWalDir: String, host: String, concurrency: Int) {
    Console.err.println("Initializing with " + concurrency + " Actors")
    val confString = """
                akka.actor.default-dispatcher{
                  type = "PinnedDispatcher"
                  executor= "thread-pool-executor"
                  thread-pool-executor{
                    max-pool-size-max =""" + concurrency + """
                  }
                }
                """
    val customConf = ConfigFactory.parseString(confString)

    implicit val system = ActorSystem("Log-Replay", ConfigFactory.load(customConf))
    implicit val timeout = Timeout(1000L * 10 * concurrency)

    val inWal = siriusLog(inWalDir)

    val reapPeriod = concurrency * 10
    var start = System.currentTimeMillis

    val httpActor = system.actorOf(Props[HttpDispatchActor].withRouter(RoundRobinRouter(nrOfInstances = concurrency)))

    val walAccumulator = inWal.foldLeft(WalAccumulator(0, List[Future[(Result)]]())) {
      case (acc: WalAccumulator, evt) => {
        def printResultsByErrorCode(cntByStatusCodeMap: Map[Int, Int], out: OutputStream) {
          Console.err.println
          if (!cntByStatusCodeMap.isEmpty) {
            Console.err.println("Response Counts by Code")
            cntByStatusCodeMap.foreach(p => {
              p._1 match {
                case 0 => Console.err.println("     Code: Err = " + p._2)
                case _ => Console.err.println("     Code: " + p._1 + " = " + p._2)
              }
            })
            Console.err.println
          } else {
            Console.err.println("No Results")
          }
          Console.err.println
        }

        //summarize every once in a while
        if (acc.totalEvents % reapPeriod == 0) {
          var sumDurations = 0
          val cntByStatusCodeMap = Map[Int, Int]().withDefaultValue(0)
          val futureList = Future.sequence(acc.futures)
          Await.result(futureList, 1000 seconds).foreach(result => result match {
            case result: Result => {
              cntByStatusCodeMap(result.statusCode) += 1
              sumDurations += result.duration
            }
            case _ => Console.err.println("Wat") //https://www.destroyallsoftware.com/talks/wat
          })
          val elapsedTime = (System.currentTimeMillis() - start).toDouble / 1000
          start = System.currentTimeMillis
          Console.err.println("------------------------------------------")
          Console.err.println("Total Time " + elapsedTime + "s")
          val rps = (reapPeriod / elapsedTime)

          printResultsByErrorCode(cntByStatusCodeMap, Console.err)

          val avgDuration = sumDurations / reapPeriod
          val errorRate = (reapPeriod - cntByStatusCodeMap(200)).toDouble / reapPeriod * 100
          Console.err.println("Avg Duration " + avgDuration + " ms for last " + reapPeriod + " requests")
          Console.err.println("Error Rate " + "%1.2f".format(errorRate) + "%")
          Console.err.println("RPS " + "%1.2f".format(rps))
          Console.err.println("------------------------------------------")
          Console.err.println
          Console.err.println

          WalAccumulator(acc.totalEvents + 1, ask(httpActor, Send(evt, host)).mapTo[Result] :: List[Future[Result]]())
        } else {
          WalAccumulator(acc.totalEvents + 1, ask(httpActor, Send(evt, host)).mapTo[Result] :: acc.futures)
        }
      }
    }

    Console.err.println("Processed " + walAccumulator.totalEvents + " OrderedEvents")
    val futureList = Future.sequence(walAccumulator.futures)
    Await.result(futureList, 100 seconds)
  }

  /**
   * returns appropriate kind of SiriusLog ie Uberstore or SegmentedUberstore
   *
   * @param inDirName location of SiriusLog
   * @return a SiriusLog of appropriate type
   */
  private def siriusLog(inDirName: String):SiriusLog = inDirName match {
       case (dir: String) if UberTool.isLegacy(dir) => UberStore(dir)
       case (dir: String) if UberTool.isSegmented(dir) => SegmentedUberStore(dir)
       case _ => throw new IllegalArgumentException(inDirName + " does not appear to be a valid Uberstore")
     }

  // TODO move this to its own class
  class HttpDispatchActor extends Actor {
    //val myHttp = Http.threads(1).configure(_.setRequestTimeoutInMs(socketTimeout).setConnectionTimeoutInMs(connTimeout).setWebSocketIdleTimeoutInMs(socketTimeout).setAllowPoolingConnection(false))

    def receive = {
      case Send(evt: OrderedEvent, host: String) => {
        Thread.sleep(20)
        val start = System.currentTimeMillis()


        val result = send(evt.request, host) match {
              case Right(status: Int) => Result((System.currentTimeMillis-start).toInt, status)
              case Left(e:Exception) => Result((System.currentTimeMillis-start).toInt, 0 )
            }
        this.sender ! result
      }
      case _ => Console.err.println("WAT WAT WAT!!!!")
    }

    private def send(request: NonCommutativeSiriusRequest, host: String) : Either[Exception,Int] = {
        val connTimeout = 100
        val socketTimeout = 800
        val uri = new URL(host + "/backend/" + request.key)
        // this does no network IO.
        val conn  = uri.openConnection().asInstanceOf[HttpURLConnection]

        request match {
          case Put(_,body) =>{
            conn.setRequestMethod("PUT")
            conn.setRequestProperty("Accept", "text/plain")
            conn.setRequestProperty("Content-Type", "application/x-protobuf")
            conn.setDoOutput(true)
            conn.setFixedLengthStreamingMode(body.length)
            conn.setReadTimeout(socketTimeout)
            conn.setConnectTimeout(connTimeout)
            try {
              conn.getOutputStream.write(body)
              Right(conn.getResponseCode)
            }
            catch{
              case e: IOException =>  Left(e)
            }
          }
          case Delete(_) => {
            conn.setRequestMethod("DELETE")
            conn.setReadTimeout(500)
            conn.setConnectTimeout(100)
            try {
              conn.getOutputStream
              Right(conn.getResponseCode)
            }
            catch{
              case e: IOException =>  Left(e)
            }
          }
        }
      }
  }
}
