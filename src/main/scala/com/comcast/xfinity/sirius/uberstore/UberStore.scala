package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import java.io.File

object UberStore {

  /**
   * Create an UberStore based in baseDir.  baseDir is NOT
   * created, it must exist. The files within baseDir will
   * be created if they do not exist however.
   *
   * @param baseDir directory to base the UberStore in
   *
   * @return an instantiated UberStore
   */
  def apply(baseDir: String): UberStore = {
    new UberStore(baseDir)
  }
}

/**
 * Expectedly high performance sequence number based append only
 * storage.
 *
 * @param baseDir directory UberStore is based i
 */
class UberStore private[uberstore] (baseDir: String) extends SiriusLog {

  var readOnlyDirs: List[UberDir] = _
  var liveDir: UberDir = _
  var nextSeq: Long = _
  init()

  /**
   * @inheritdoc
   */
  def writeEntry(event: OrderedEvent) {
    if (event.sequence < nextSeq) {
      throw new IllegalArgumentException("May not write event out of order, expected sequence " + nextSeq + " but received " + event.sequence)
    }
    liveDir.writeEntry(event)
    nextSeq = event.sequence + 1
  }

  /**
   * @inheritdoc
   */
  def getNextSeq = nextSeq

  /**
   * @inheritdoc
   */
  def foldLeftRange[T](startSeq: Long, endSeq: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T = {
    val res0 = readOnlyDirs.foldLeft(acc0)(
      (acc, dir) => dir.foldLeftRange(startSeq, endSeq)(acc)(foldFun)
    )
    liveDir.foldLeftRange(startSeq, endSeq)(res0)(foldFun)
  }


  /**
   * Close underlying file handles or connections.  This UberStore should not be used after
   * close is called.
   */
  def close() {
    liveDir.close()
    readOnlyDirs.foreach(_.close())
  }

  /**
   * Consider this closed if either of its underlying objects are closed, no good writes
   * will be able to go through in that case.
   *
   * @return whether this is "closed," i.e., unable to be written to
   */
  def isClosed = liveDir.isClosed


  // gross mutable code, but a necessary part of life...
  private def init() {
    val dirs = new File(baseDir).listFiles.toList.collect {
      case f if f.isDirectory && f.getName.forall(_.isDigit) => f.getName
    } sortWith (_.toLong > _.toLong)

    val (initLiveDir, initReadOnlyDirs) = dirs match {
      case Nil => (UberDir(baseDir, "1"), Nil)
      case hd :: tl => (UberDir(baseDir, hd), tl.reverse.map(UberDir(baseDir, _)))
    }

    liveDir = initLiveDir
    readOnlyDirs = initReadOnlyDirs
    // liveDir may be empty, so we need to check the last read only dir,
    //  checking all and getting max is just more succinct in terms of LOC...
    nextSeq = (liveDir.getNextSeq :: readOnlyDirs.map(_.getNextSeq)).max
  }

}
