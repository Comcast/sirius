package com.comcast.xfinity.sirius.tool

import java.io._

import scala.util.matching.Regex

import com.comcast.xfinity.sirius.api.impl._
import com.comcast.xfinity.sirius.tool.format.OrderedEventFormatter
import com.comcast.xfinity.sirius.uberstore.UberStore
import com.comcast.xfinity.sirius.uberstore.UberTool
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog


import akka.dispatch.{Await, Future}
import scala.collection.mutable.Map
import akka.actor.{ActorSystem, Props, Actor}
import akka.routing.RoundRobinRouter
import akka.pattern.ask
import akka.util.duration._
import com.typesafe.config.ConfigFactory


import dispatch._


import akka.util.Timeout
import java.net.{ProtocolException, MalformedURLException, HttpURLConnection, URL}
import scala._
import java.lang.String

import org.apache.http.util.EntityUtils
import org.apache.commons.io.IOUtils
import com.comcast.xfinity.sirius.api.impl.Put
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.Delete
import scala.Left
import com.comcast.xfinity.sirius.api.impl.Put
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import scala.Right
import scala.Console
import com.comcast.xfinity.sirius.api.impl.Delete

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
    Console.err.println("       are fully removed from the resultant log.")
    Console.err.println()
    Console.err.println("   tail [-n number] [-f] <walDir>")
    Console.err.println("       Show last 20 sequence numbers in wal.")
    Console.err.println("       -n: number of lines to show (e.g., -n 10, -n 50), defaults to 20")
    Console.err.println("       -f option enables follow mode.  Break follow mode with ^C.")
    Console.err.println("       (follow mode is recommended, since wal needs to be initialized")
    Console.err.println("        with each instantiation, which takes a few seconds")
    Console.err.println()
    Console.err.println("   range <begin> <end> <walDir>")
    Console.err.println("       Show all entries in begin to end range, inclusive")
    Console.err.println()
    Console.err.println("   keyFilter <regexp> <inWalDir> <outWalDir>")
    Console.err.println("       Write all OrderedEvents in UberStore in inWalDir having a key")
    Console.err.println("       matching regexp to new UberStore in outWalDir")
    Console.err.println()
    Console.err.println("   keyFilterNot <regexp> <inWalDir> <outWalDir>")
    Console.err.println("       Same as keyFilter, except the resulting UberStore contains all")
    Console.err.println("       OrderedEvents not matching regexp")
    Console.err.println()
    Console.err.println("   replay <inWalDir> <host> <concurrency>")
    Console.err.println("       For each OrderedEvent will issue an http request")

  }

  def main(args: Array[String]) {
    args match {
      case Array("compact", inWalDirName, outWalDirName) =>
        compact(inWalDirName, outWalDirName, false)
      case Array("compact", "two-pass", inWalDirName, outWalDirName) =>
        compact(inWalDirName, outWalDirName, true)
      case Array("tail", walDir) =>
        tailUber(walDir)
      case Array("tail", "-n", number, walDir) =>
        tailUber(walDir, number.toInt)
      case Array("range", begin, end, walDir) =>
        printSeq(UberStore(walDir), begin.toLong, end.toLong)
      case Array("keyFilter", regexpStr, inWal, outWal) =>
        val regexp = regexpStr.r
        val filterFun: OrderedEvent => Boolean = keyMatch(regexp, _)
        filter(inWal, outWal, filterFun)
      case Array("keyFilterNot", regexpStr, inWal, outWal) =>
        val regexp = regexpStr.r
        val filterFun: OrderedEvent => Boolean = !keyMatch(regexp, _)
        filter(inWal, outWal, filterFun)
      case Array("replayLog", inWal, host, concurrency) =>
        replay(inWal, host, concurrency.toInt)
      case _ => printUsage()
    }
    sys.exit(0)
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
   * @param follow whether to follow, printing the last n lines every sleepDuration ms
   * @param sleepDuration number of ms between prints in follow mode
   */
  private def tailUber(inDirName: String, number: Int = 20) {
    val wal = UberStore(inDirName)
    var seq = wal.getNextSeq - 1

    printSeq(wal, seq - number, seq)
  }

  /**
   * Internal helper for tailUber, does the actual printing of a range.
   *
   * @param wal uberstore to target
   * @param first first seq to print
   * @param last last seq to print
   */
  private def printSeq(wal: UberStore, first: Long, last: Long) {
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

    val inWal = UberStore(inUberDir)
    val outWal = UberStore(outUberDir)

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
    // XXX: I know this is side effectful, deal with it, a foreach is probably a good
    //      thing to put back on SiriusLog.
    inWal.foldLeft(outWal) {
      case (wal, evt) if pred(evt) => wal.writeEntry(evt); wal
      case (wal, _) => wal
    }
  }

  /**
   * Generates PUT/DELETE traffic from a WAL
   *
   * @param inWalDir dir for write ahead log
   * @param host where load should be directed
   * @param concurrency approximate concurrency desired
   */
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

    val inWal = UberStore(inWalDir)


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





  case class WalAccumulator(totalEvents: Int, futures: List[Future[Result]])


  case class Send(evt: OrderedEvent, host: String)

  case class Result(duration: Int, statusCode: Int)




}