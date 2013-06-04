package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import java.io.File
import scala.collection.mutable.WrappedArray
import scala.collection.mutable.{HashMap => MutableHashMap}
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.Put
import scalax.file.Path
import java.util

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
  var currentlyCompacting = false
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

  /**
   * Create a new liveDir, based on the current max dirNum + 1. Move current
   * liveDir into read-only mode.
   *
   * Currently do NOT support a split while currently compacting.
   *
   * @return new uberdir name
   */
  def split(): String = {
    if (!currentlyCompacting) {
      readOnlyDirs :+= liveDir
      val newDirNum = readOnlyDirs.map(_.name.toLong).max + 1
      liveDir = UberDir(baseDir, newDirNum.toString)
    }

    liveDir.name
  }

  private def gatherCompactionOffsets(): Array[Long] = {
    val toKeep = MutableHashMap[WrappedArray[Byte], Long]()
    var offset = 0L
    // developer's note- it may be nice to not fully deserialize all of these...
    // we could use a decoder that just returns WrappedArrays of keys instead of full OrderedEvents...
    readOnlyDirs.foreach(d => d.foreach {
      case OrderedEvent(_, _, Delete(key)) =>
        toKeep.put(WrappedArray.make(key.getBytes), offset)
        offset += 1
      case OrderedEvent(_, _, Put(key, _)) =>
        toKeep.put(WrappedArray.make(key.getBytes), offset)
        offset += 1
    })

    // XXX built up this whole map, but only need offset array
    val keepableOffsets = toKeep.values.toArray
    util.Arrays.sort(keepableOffsets)

    keepableOffsets
  }

  private def writeEventsByOffset(offsets: Array[Long], output: UberDir) {
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
          index += 1
        }
      ))
    }
  }

  def compact() {
    val seq = split()

    currentlyCompacting = true
    val compactionOffsets = gatherCompactionOffsets()

    val compactInto = UberDir(baseDir, "compacting")
    writeEventsByOffset(compactionOffsets, compactInto)
    compactInto.close()

    val compacting = new File(baseDir, "compacting")
    val compacted = new File(baseDir, "compacted.pre-%s".format(seq))

    compacting.renameTo(compacted)

    // XXX cannot just set new UberDir and then rename underneath it,
    // UberDir hangs onto the path String instead of a filehandle. Actually, even
    // if it had a real filehandle, it doesn't follow a rename. Balls.

    // It'd be really nice just to lock everything down and do the rest of this
    // as a single atomic action, being interrupted in this next bit is bad-scary.
    // In any case, we're just unlinking a few files, and moving another one. It should be lightning quick.

    // This will work, and if it dies in the middle, it will be manually-recoverable.
    // We could make it automatically recoverable, though that gets a little hairy.

    // Currently setting no RO directories while we do the swap, so nobody can be in the middle of
    // reading while they delete. Unless they already were reading, in which case Bad Things will
    // probably happen (their operation will be borked and they'll have to restart it).
    val oldReadOnlyDirs = readOnlyDirs
    readOnlyDirs = Nil
    for (dir <- oldReadOnlyDirs) {
      dir.close()
      Path.fromString(baseDir + "/" + dir.name).deleteRecursively()
    }

    compacted.renameTo(new File(baseDir, "1"))
    readOnlyDirs = List(UberDir(baseDir, "1"))
    currentlyCompacting = false
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
