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
package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import scala.collection.mutable.{HashMap => MutableHashMap}
import com.comcast.xfinity.sirius.api.impl.{Put, Delete, OrderedEvent}
import scala.collection.mutable.WrappedArray
import java.util.Arrays
import java.io.File
import com.comcast.xfinity.sirius.uberstore.segmented.SegmentedUberStore

object UberTool {

  def isLegacy(walDirName: String): Boolean =
    new File(walDirName, "1.data").exists()

  def isSegmented(walDirName: String): Boolean =
    new File(walDirName, SegmentedUberStore.versionId).exists()

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
   * @param deleteCutoff deletes with a timestamp before this
   *          point are completely removed from the log. defaults
   *          to 0, meaning any deletes from 12AM Jan 1, 1970 or
   *          before are completely removed from the log
   */
  def compact(inLog: SiriusLog, outLog: SiriusLog, deleteCutoff: Long = 0) {
    val toKeep = new MutableHashMap[WrappedArray[Byte], OrderedEvent]

    inLog.foldLeft(()) {
      case (_, evt @ OrderedEvent(_, ts, Delete(key))) =>
        if (ts <= deleteCutoff) {
          toKeep.remove(WrappedArray.make(key.getBytes))
        } else {
          toKeep.put(WrappedArray.make(key.getBytes), evt)
        }
      case (_, evt @ OrderedEvent(_, _, Put(key, _))) =>
        toKeep.put(WrappedArray.make(key.getBytes), evt)
    }

    val toWrite = new Array[OrderedEvent](toKeep.size)
    var i = 0
    toKeep.foreach(kv => { toWrite(i) = kv._2; i += 1 })

    toWrite.sortWith(_.sequence < _.sequence).foreach(outLog.writeEntry(_))
  }

  /**
   * Also not perfect, and slower than single pass, but this compaction implementation
   * will run with a more reasonable memory footprint.
   *
   * @param inLog input log
   * @param outLog output log, needs to be empty (or you're likely to get an
   *               "out of order write" exception)
   * @param deleteCutoff deletes with a timestamp before this
   *          point are completely removed from the log. defaults
   *          to 0, meaning any deletes from 12AM Jan 1, 1970 or
   *          before are completely removed from the log
   */
  def twoPassCompact(inLog: SiriusLog, outLog: SiriusLog, deleteCutoff: Long = 0) {
    // Pass 1: get offsets of all keepable events (ie: last delete/put per key)
    val keepableOffsetIterator = gatherKeepableEventOffsets(inLog, deleteCutoff).iterator

    // Pass 2: write it out, skip if there's nothing worth keeping, saves time
    //          and energy- Sirius is a green product
    if (!keepableOffsetIterator.isEmpty) {
      writeKeepableEvents(inLog, outLog, keepableOffsetIterator)
    }
  }

  private def gatherKeepableEventOffsets(inLog: SiriusLog, deleteCutoff: Long) = {
    val toKeep = new MutableHashMap[WrappedArray[Byte], Long]()

    // generate map of Id (Key) -> EntryNum (logical offset)
    var index = 1
    inLog.foldLeft(()) {
      case (_, OrderedEvent(_, ts, Delete(key))) =>
        if (ts <= deleteCutoff) {
          toKeep.remove(WrappedArray.make(key.getBytes))
        } else {
          toKeep.put(WrappedArray.make(key.getBytes), index)
        }
        index += 1
      case (_, OrderedEvent(_, _, Put(key, _))) =>
        toKeep.put(WrappedArray.make(key.getBytes), index)
        index += 1
    }

    // keys are useless, only need offsets, sorted
    val keepableOffsets = new Array[Long](toKeep.size)
    var i = 0
    toKeep.foreach(kv => { keepableOffsets(i) = kv._2; i += 1})
    Arrays.sort(keepableOffsets)
    keepableOffsets
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

}
