package com.comcast.xfinity.sirius.tool

import com.comcast.xfinity.sirius.uberstore.{UberStore, UberTool}
import java.io.File
import com.comcast.xfinity.sirius.writeaheadlog.{SiriusLog, SiriusFileLog}

/**
 * Object meant to be invoked as a main class from the terminal.  Provides some
 * simple write ahead log operations.
 *
 * See usage for usage
 */
object WalTool {

  private def printUsage() {
    Console.err.println("Usage:")
    Console.err.println("   convert-to-uber <inWalName> <outWalDirName>")
    Console.err.println("       Convert a legacy (line based) log to an UberStore.")
    Console.err.println()
    Console.err.println("   convert-to-legacy <inWalDir> <outWalName>")
    Console.err.println("       Convert an UberStore into a legacy (line based) log")
    Console.err.println()
    Console.err.println("   compact [two-pass] <inWalDir> <outWalDir>")
    Console.err.println("       Compact UberStore in inWalDir into new UberStore in outWalDir")
    Console.err.println("       If two-pass is provided then the more memory efficient two pass")
    Console.err.println("       compaction algorithm will be used")
    Console.err.println()
    Console.err.println("   compact-legacy [two-pass] <inWalDir> <outWalDir>")
    Console.err.println("       Compact a legacy (line based) log into another")
    Console.err.println("       If two-pass is provided then the more memory efficient two pass")
    Console.err.println("       compaction algorithm will be used")
  }

  def main(args: Array[String]) {
    args match {
      case Array("convert-to-uber", inWalName, outWalDirName) =>
        convertToUber(inWalName, outWalDirName)

      case Array("convert-to-legacy", inWalDir, outWalName) =>
        convertToLegacy(inWalDir, outWalName)

      case Array("compact", inWalDirName, outWalDirName) =>
        compact(inWalDirName, outWalDirName, false)
      case Array("compact", "two-pass", inWalDirName, outWalDirName) =>
        compact(inWalDirName, outWalDirName, true)

      case Array("compact-legacy", inWalName, outWalName) =>
        compactLegacy(inWalName, outWalName, false)
      case Array("compact-legacy", "two-pass", inWalName, outWalName) =>
        compactLegacy(inWalName, outWalName, true)

      case _ => printUsage()
    }
    sys.exit(0)
  }

  /**
   * Convert a SiriusFileLog specified by inWalName to an UberStore
   * in outWalDirName. outWalDirName must not exist.
   *
   * @param inWalName file name specifying SiriusFileLog
   * @param outWalDirName output directory name specifying where to base
   *          new UberStore
   */
  private def convertToUber(inWalName: String, outWalDirName: String) {
    val inWal = SiriusFileLog(inWalName)

    createFreshDir(outWalDirName)
    val outWal = UberStore(outWalDirName)

    UberTool.copyLog(inWal, outWal)
  }

  /**
   * Convert an UberStore specified by inWalDirName to a SiriusFileLog
   * in outWalName.
   *
   * @param inDirName directory specifying UberStore
   * @param outWalName file name specifying SiriusFileLog
   */
  private def convertToLegacy(inWalDirName: String, outWalName: String) {
    val inWal = UberStore(inWalDirName)
    val outWal = SiriusFileLog(outWalName)
    UberTool.copyLog(inWal, outWal)
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
   * Compact a SiriusFileLog
   *
   * @param inWalName file name specifying input SiriusFileLog
   * @param outWalName directory specifying output SiriusFileLog
   * @param twoPass true to use slower, more memory efficient twoPass algorithm,
   *          false to use faster, more memory intensive algorithm
   */
  private def compactLegacy(inWalName: String, outWalName: String, twoPass: Boolean) {
    val inWal = SiriusFileLog(inWalName)
    val outWal = SiriusFileLog(outWalName)

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
}