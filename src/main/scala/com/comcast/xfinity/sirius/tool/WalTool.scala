package com.comcast.xfinity.sirius.tool

import java.io.File

import scala.util.matching.Regex

import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.Put
import com.comcast.xfinity.sirius.tool.format.OrderedEventFormatter
import com.comcast.xfinity.sirius.uberstore.UberStore
import com.comcast.xfinity.sirius.uberstore.UberTool
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import scalax.file.Path

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
    Console.err.println("   convertUberStore <inWalDir> <outWalDir>")
    Console.err.println("       Convert a Legacy UberStore into a new (dir-based) UberStore")

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

      case Array("convertUberStore", inWal, outWal) =>
        convertUberStore(inWal, outWal)

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

  private def convertUberStore(inUberStore: String, outUberStore: String) {
    val inData = Path.fromString(inUberStore + "/1.data")
    val inIndex = Path.fromString(inUberStore + "/1.index")
    if (!(inData.exists && inData.isFile)) {
      throw new Exception("Could not find input data in directory: %s, aborting.".format(inUberStore))
    }

    val outDir = Path.fromString(outUberStore + "/1")
    val outData = Path.fromString(outUberStore + "/1/data")
    val outIndex = Path.fromString(outUberStore + "/1/index")
    if (outDir.exists) {
      throw new Exception("Directory that looks like new-style UberDir directory already exists at %s, aborting.".format(outDir))
    }

    outDir.createDirectory(createParents = true)
    inData.moveTo(outData)
    inIndex.moveTo(outIndex)
  }
}