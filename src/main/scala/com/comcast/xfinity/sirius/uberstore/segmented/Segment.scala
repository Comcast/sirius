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
package com.comcast.xfinity.sirius.uberstore.segmented

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.uberstore.seqindex.{SeqIndex, DiskOnlySeqIndex}
import com.comcast.xfinity.sirius.uberstore.data.UberDataFile
import java.io.File

object Segment {

  /**
   * Create an Segment based in baseDir named "name".
   *
   * @param base directory containing the Segment
   * @param name the name of this dir
   *
   * @return an Segment instance, fully repaired and usable
   */
  def apply(base: File, name: String): Segment = {
    apply(new File(base, name))
  }

  /**
   * Create an Segment at the specified location.
   *
   * @param location location of segment. must be inside a segmented uberstore's base
   *                 to be meaningful.
   *
   * @return an Segment instance, fully repaired and usable
   */
  def apply(location: File): Segment = {
    location.mkdirs()

    val dataFile = new File(location, "data")
    val indexFile = new File(location, "index")
    val compactionFlagFile = new File(location, "keys-collected")
    val internalCompactionFlagFile = new File(location, "internally-compacted")

    val data = UberDataFile(dataFile.getAbsolutePath)
    val index = DiskOnlySeqIndex(indexFile.getAbsolutePath)
    val compactionFlag = FlagFile(compactionFlagFile.getAbsolutePath)
    val internalCompactionFlag = FlagFile(internalCompactionFlagFile.getAbsolutePath)

    repairIndex(index, data)
    new Segment(location, location.getName, data, index, compactionFlag, internalCompactionFlag)
  }

  /**
   * Recovers missing entries at the end of index from dataFile.
   *
   * Assumes that UberDataFile is proper, that is events are there in order,
   * and there are no dups.
   *
   * Has the side effect of updating index.
   *
   * @param index the SeqIndex to update
   * @param dataFile the UberDataFile to update
   */
  private[segmented] def repairIndex(index: SeqIndex, dataFile: UberDataFile) {
    val (includeFirst, lastOffset) = index.getMaxSeq match {
      case None => (true, 0L)
      case Some(seq) => (false, index.getOffsetFor(seq).get) // has to exist
    }

    dataFile.foldLeftRange(lastOffset, Long.MaxValue)(includeFirst) (
      (shouldInclude, off, evt) => {
        if (shouldInclude) {
          index.put(evt.sequence, off)
        }
        true
      }
    )

  }
}

/**
 * Expectedly high performance sequence number based append only
 * storage directory.  Stores all data in dataFile, and sequence -> data
 * mappings in index.
 *
 * @param dataFile the UberDataFile to store data in
 * @param index the SeqIndex to use
 */
class Segment private[uberstore](val location: File, val name: String, dataFile: UberDataFile,
                                 index: SeqIndex, compactionFlag: FlagFile, internalCompactionFlag: FlagFile) {

  /**
   * Get the number of entries written to the Segment
   * @return number of entries in the Segment
   */
  def size = index.size

  /**
   * Write OrderedEvent event into this dir. Will fail if closed or sequence
   * is out of order.
   *
   * @param event the {@link OrderedEvent} to write
   */
  def writeEntry(event: OrderedEvent) {
    if (isClosed) {
      throw new IllegalStateException("Attempting to write to closed Segment")
    }
    if (event.sequence < getNextSeq) {
      throw new IllegalArgumentException("Writing events out of order is bad news bears")
    }
    val offset = dataFile.writeEvent(event)
    index.put(event.sequence, offset)
  }

  /**
   * Get all the unique keys in the segment.
   *
   * @return set of keys in this Segment
   */
  def keys: Set[String] =
    foldLeft(Set[String]())(
      (acc, event) => acc + event.request.key
    )

  /**
   * Get the next possible sequence number in this dir.
   */
  def getNextSeq = index.getMaxSeq match {
    case None => 1L
    case Some(seq) => seq + 1
  }

  def foreach[T](fun: OrderedEvent => T) {
    foldLeft(())((_, evt) => fun(evt))
  }

  /**
   * Fold left over all entries.
   *
   * @param acc0 initial accumulator value
   * @param foldFun the fold function
   */
  def foldLeft[T](acc0: T)(foldFun: (T, OrderedEvent) => T): T =
    foldLeftRange(0, Long.MaxValue)(acc0)(foldFun)

  /**
   * Fold left over a range of entries based on sequence number.
   */
  def foldLeftRange[T](startSeq: Long, endSeq: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T = {
    val (startOffset, endOffset) = index.getOffsetRange(startSeq, endSeq)
    dataFile.foldLeftRange(startOffset, endOffset)(acc0)(
      (acc, _, evt) => foldFun(acc, evt)
    )
  }

  /**
   * Close underlying file handles or connections.  This Segment should not be used after
   * close is called.
   */
  def close() {
    if (!dataFile.isClosed) {
      dataFile.close()
    }
    if (!index.isClosed) {
      index.close()
    }
  }

  /**
   * Consider this closed if either of its underlying objects are closed, no good writes
   * will be able to go through in that case.
   *
   * @return whether this is "closed," i.e., unable to be written to
   */
  def isClosed = dataFile.isClosed || index.isClosed

  /**
   * Have the keys in this segment been applied via compaction to previous segments?
   * @return true if the keys have been compacted away from previous segments, false otherwise
   */
  def isApplied = compactionFlag.value

  /**
   * Set the keys-applied flag.
   * @param applied the value of the flag
   */
  def setApplied(applied: Boolean) {
    compactionFlag.set(value = applied)
  }

  /**
   * Has this segment been internally compacted?
   *
   * @return true if internal compaction is completed, false otherwise
   */
  def isInternallyCompacted = internalCompactionFlag.value

  /**
   * Set the internally-compacted flag
   *
   * @param compacted the value of the flag
   */
  def setInternallyCompacted(compacted: Boolean) {
    internalCompactionFlag.set(compacted)
  }
}
