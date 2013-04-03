package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import scala.collection.mutable.{HashMap => MutableHashMap}
import com.comcast.xfinity.sirius.api.impl.{Put, Delete, OrderedEvent}

object UberTool {

  /**
   * Copies data from inFile to outFile.  outFile is appended to.
   *
   * Has the side effect of writing data into outFile.
   *
   * @param inLog the SiriusLog to copy data from, this file is
   *          not modified
   * @param outLog the SiriusLog to copy data into
   */
  def copyLog(inLog: SiriusLog, outLog: SiriusLog) {
    inLog.foldLeft(())(
      (_, evt) => outLog.writeEntry(evt)
    )
  }

  /**
   * Compacts events in inFile into outFile, appending.
   *
   * Has the side effects of writing data into outFile.
   *
   * NOTE: this is an unoptimized, high level compaction
   * algorithm and can take a lot of memory.  Make sure your
   * JVM is configured properly when you use this.
   *
   * @param inLog the SiriusLog to compact events from
   * @param outLog the SiriusLog to write the compacted
   *          log into
   */
  def compact(inLog: SiriusLog, outLog: SiriusLog) {
    val toKeep = new MutableHashMap[String, OrderedEvent]

    inLog.foldLeft(())(
      (_, evt) => {
        toKeep.put(keyFromEvent(evt), evt)
      }
    )

    val toWrite = toKeep.values.toList.sortWith(_.sequence < _.sequence)

    toWrite.foreach(outLog.writeEntry(_))
  }

  /**
   * Also not perfect, and slower than single pass, but this compaction implementation
   * will run with a more reasonable memory footprint.
   *
   * @param inLog input log
   * @param outLog output log, needs to be empty (or you're likely to get an
   *               "out of order write" exception)
   */
  def twoPassCompact(inLog: SiriusLog, outLog: SiriusLog) {
    // Pass 1: get offsets of all keepable events (ie: last delete/put per key)
    val keepableOffsetIterator = gatherKeepableEventOffsets(inLog).iterator

    // Pass 2: write it out, skip if there's nothing worth keeping, saves time
    //          and energy- Sirius is a green product
    if (!keepableOffsetIterator.isEmpty) {
      writeKeepableEvents(inLog, outLog, keepableOffsetIterator)
    }
  }

  private def gatherKeepableEventOffsets(inLog: SiriusLog) = {
    val toKeep = new MutableHashMap[String, Long]

    // generate map of Id (Key) -> EntryNum (logical offset)
    var index = 1
    inLog.foldLeft(())(
      (_, evt) => {
        toKeep.put(keyFromEvent(evt), index)
        index += 1
      }
    )

    // keys are useless, only need offsets, sorted
    toKeep.values.toList.sortWith(_ < _)
  }

  // don't call with an empty iterator- you will have a bad time
  private def writeKeepableEvents(inLog: SiriusLog, outLog: SiriusLog, toWriteIterator: Iterator[Long]) {
    var nextWrite = toWriteIterator.next()

    // write events whose positions appear in toWriteIterator
    var index = 1
    inLog.foldLeft(())(
      (_, evt) => {
        if (index == nextWrite) {
          outLog.writeEntry(evt)
          if (toWriteIterator.hasNext) {
            nextWrite = toWriteIterator.next()
          }
        }
        index += 1
      }
    )
  }

  private def keyFromEvent(evt: OrderedEvent): String = evt.request match {
    case Put(key, _) => key
    case Delete(key) => key
  }
}