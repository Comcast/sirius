package com.comcast.xfinity.sirius.uberstore.segmented

import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import java.io.File
import scala.collection.mutable.WrappedArray
import scala.collection.mutable.{HashMap => MutableHashMap}
import scalax.file.Path
import java.util
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.Put
import com.comcast.xfinity.sirius.uberstore.UberStore._

object SegmentedUberStore {

  /**
   * Create an SegmentedUberStore based in baseDir.  baseDir is NOT
   * created, it must exist. The files within baseDir will
   * be created if they do not exist however.
   *
   * @param baseDir directory to base the SegmentedUberStore in
   *
   * @return an instantiated SegmentedUberStore
   */
  def apply(baseDir: String): SegmentedUberStore = {
    new SegmentedUberStore(baseDir)
  }
}

/**
 * Expectedly high performance sequence number based append only
 * storage.
 *
 * @param baseDir directory SegmentedUberStore is based i
 */
class SegmentedUberStore private[segmented] (baseDir: String) extends SiriusLog {

  // XXX incorporate maxDeleteAge
  // val maxDeleteAge = System.getProperty("maxDeleteAge", "604800000").toLong
  var readOnlyDirs: List[Segment] = _
  var liveDir: Segment = _
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
   * Close underlying file handles or connections.  This SegmentedUberStore should not be used after
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

  /**
   * Unimplemented. Come back later.
   */
  def compact() {
    throw new UnsupportedOperationException("compact is not yet supported on SegmentedUberStore")
  }

  var state: CompactionState = NotCompacting
  /**
   * Never compacting, since compact() is unimplemented. Come back later.
   * @return State of current compaction
   */
  def getCompactionState = state

  // gross mutable code, but a necessary part of life...
  private def init() {
    val dirs = new File(baseDir).listFiles.toList.collect {
      case f if f.isDirectory && f.getName.forall(_.isDigit) => f.getName
    } sortWith (_.toLong > _.toLong)

    val (initLiveDir, initReadOnlyDirs) = dirs match {
      case Nil => (Segment(baseDir, "1"), Nil)
      case hd :: tl => (Segment(baseDir, hd), tl.reverse.map(Segment(baseDir, _)))
    }

    liveDir = initLiveDir
    readOnlyDirs = initReadOnlyDirs
    // liveDir may be empty, so we need to check the last read only dir,
    //  checking all and getting max is just more succinct in terms of LOC...
    nextSeq = (liveDir.getNextSeq :: readOnlyDirs.map(_.getNextSeq)).max
  }

}
