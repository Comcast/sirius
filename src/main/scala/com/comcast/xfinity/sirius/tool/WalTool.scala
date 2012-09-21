package com.comcast.xfinity.sirius.tool

import com.comcast.xfinity.sirius.uberstore.{UberStore, UberTool}
import java.io.File
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.api.impl.persistence.BoundedLogRange
import com.comcast.xfinity.sirius.uberstore.seqindex.ReadOnlySeqIndex
import com.comcast.xfinity.sirius.uberstore.data.UberDataFile

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
    Console.err.println("       compaction algorithm will be used")
    Console.err.println()
    Console.err.println("   tail [-n number] [-f] <walDir>")
    Console.err.println("       Show last 20 sequence numbers in wal.")
    Console.err.println("       -n: number of lines to show (e.g., -n 10, -n 50), defaults to 20")
    Console.err.println("       -f option enables follow mode.  Break follow mode with ^C.")
    Console.err.println("       (follow mode is recommended, since wal needs to be initialized")
    Console.err.println("        with each instantiation, which takes a few seconds")
  }

  def main(args: Array[String]) {
    args match {
      case Array("compact", inWalDirName, outWalDirName) =>
        compact(inWalDirName, outWalDirName, false)
      case Array("compact", "two-pass", inWalDirName, outWalDirName) =>
        compact(inWalDirName, outWalDirName, true)

      case Array("tail", walDir) =>
        tailUber(walDir)
      case Array("tail", "-f", walDir) =>
        tailUber(walDir, follow = true)
      case Array("tail", "-n", number, walDir) =>
        tailUber(walDir, number.toInt)
      case Array("tail", "-n", number, "-f", walDir) =>
        tailUber(walDir, number.toInt, follow = true)

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
    if (twoPass) {
      UberTool.twoPassCompact(inWal, outWal)
    } else {
      UberTool.compact(inWal, outWal)
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
  private def tailUber(inDirName: String, number: Int = 20, follow: Boolean = false, sleepDuration: Int = 1000) {
    val wal = new UberStore(UberDataFile(new File(inDirName, "1.data").getAbsolutePath),
                            ReadOnlySeqIndex(new File(inDirName, "1.index").getAbsolutePath))
    var seq = wal.getNextSeq - 1

    printSeq(wal, seq - number, seq)
    while (follow) {
      Thread.sleep(sleepDuration)
      print("\033["+"2J")
      seq = wal.getNextSeq - 1
      printSeq(wal, seq - number, seq)
    }
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
      println(event)
    )
  }

}