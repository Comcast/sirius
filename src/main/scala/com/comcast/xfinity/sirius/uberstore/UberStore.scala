package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import java.io.File
import scala.collection.mutable.WrappedArray
import scala.collection.mutable.{HashMap => MutableHashMap}
import scalax.file.Path
import java.util
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.Put

object UberStore {

  sealed trait CompactionState
  case object NotCompacting extends CompactionState
  case class Compacting(stateType: StateType, progress: Long) extends CompactionState

  sealed trait StateType
  case object GatheringEvents extends StateType
  case object WritingEvents extends StateType

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
  import UberStore._

  val eventsBetweenCompactionUpdates = 1000000L
  var state: CompactionState = NotCompacting

  // XXX incorporate maxDeleteAge
  // val maxDeleteAge = System.getProperty("maxDeleteAge", "604800000").toLong
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

  def getCompactionState = state

  /**
   * Compact the current read-only dirs into a single dir.
   *
   * This method has two side-effects:
   * - it collapses the multiple read-only dirs into one
   * - it removes entries from the resulting WAL that are overridden by later
   *   entries in the WAL. For a given key, only the latest event is kept.
   */
  def compact() = synchronized {
    // split off a new liveDir so we can compact what's currently live
    split()

    // we're compacting readOnlyDirs, which is 1 through maxDir
    val maxDir = readOnlyDirs.map(_.name.toLong).max
    // actually do the compaction, putting it into a compacted.1-$maxDir directory
    val compacted = doCompact(readOnlyDirs, "compacted.1-%s".format(maxDir))
    // replace the current readOnlyDirs with the newly-created compacted.1-$maxDir
    replace(readOnlyDirs, compacted)

    state = NotCompacting
  }

  /**
   * Create a new liveDir, based on the current max dirNum + 1. Move current
   * liveDir into read-only mode.
   *
   * Currently do NOT support a split while compacting.
   */
  private def split() {
    readOnlyDirs :+= liveDir
    val newDirNum = readOnlyDirs.map(_.name.toLong).max + 1
    liveDir = UberDir(baseDir, newDirNum.toString)
  }

  private def doCompact(toCompact: List[UberDir], dirName: String): File = {
    val compactionOffsets = gatherCompactionOffsets()

    val compactInto = UberDir(baseDir, "compacting")
    writeEventsByOffset(compactionOffsets, compactInto)
    compactInto.close()

    val compacting = new File(baseDir, "compacting")
    val compacted = new File(baseDir, dirName)

    compacting.renameTo(compacted)

    compacted
  }

  /**
   * HAS SIDE EFFECTS! Changes readOnlyDirs.
   *
   * Replace removes the current readOnlyDirs, and replaces them with a newly-compacted UberDir
   *
   * The old dirs are DELETED.
   *
   * @param toReplace uberdirs to replace
   * @param replaceWith filehandle of the replacement
   */
  private def replace(toReplace: List[UberDir], replaceWith: File) {
    readOnlyDirs = Nil
    for (dir <- toReplace) {
      dir.close()
      Path.fromString(baseDir + "/" + dir.name).deleteRecursively()
    }

    replaceWith.renameTo(new File(baseDir, "1"))
    readOnlyDirs = List(UberDir(baseDir, "1"))
  }


  private def gatherCompactionOffsets(): Array[Long] = {
    val toKeep = MutableHashMap[WrappedArray[Byte], Long]()
    var offset = 0L
    state = Compacting(GatheringEvents, 0L)

    // developer's note- it may be nice to not fully deserialize all of these...
    // we could use a decoder that just returns WrappedArrays of keys instead of full OrderedEvents...
    readOnlyDirs.foreach(d => d.foreach {
      case OrderedEvent(_, ts, Delete(key)) =>
        toKeep.put(WrappedArray.make(key.getBytes), offset)

        if (offset % eventsBetweenCompactionUpdates == 0) {
          state = Compacting(GatheringEvents, offset)
        }
        offset += 1
      case OrderedEvent(_, _, Put(key, _)) =>
        toKeep.put(WrappedArray.make(key.getBytes), offset)

        if (offset % eventsBetweenCompactionUpdates == 0) {
          state = Compacting(GatheringEvents, offset)
        }
        offset += 1
    })

    // built up this whole map, but only need offset array
    val keepableOffsets = toKeep.values.toArray
    util.Arrays.sort(keepableOffsets)

    keepableOffsets
  }

  private def writeEventsByOffset(offsets: Array[Long], output: UberDir) {
    state = Compacting(WritingEvents, 0L)

    val toWriteIterator = offsets.iterator
    if (!toWriteIterator.isEmpty) {
      var nextWrite = toWriteIterator.next()
      // start again
      var index = 0L
      readOnlyDirs.foreach(d => d.foreach(
        evt => {
          if (index == nextWrite) {
            output.writeEntry(evt)
            if (toWriteIterator.hasNext) {
              nextWrite = toWriteIterator.next()
            }
          }

          if (index % eventsBetweenCompactionUpdates == 0) {
            state = Compacting(WritingEvents, index)
          }
          index += 1
        }
      ))
    }
  }

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
