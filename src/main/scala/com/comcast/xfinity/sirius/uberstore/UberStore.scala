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

import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.uberstore.segmented.SegmentedUberStore
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
    if (!isValidUberStore(baseDir)) {
      throw new IllegalStateException("Cannot start. Configured to boot with legacy UberStore, but other UberStore format found.")
    }

    new UberStore(baseDir, UberPair(baseDir, 1L))
  }

  /**
   * There's no explicit marker for Legacy UberStores. All we can do is check that it's not any of
   * the other known varieties.
   *
   * @param baseDir directory UberStore is based in
   * @return true if baseDir corresponds to a valid legacy UberStore, false otherwise
   */
  def isValidUberStore(baseDir: String): Boolean = {
    !UberTool.isSegmented(baseDir)
  }
}

/**
 * Expectedly high performance sequence number based append only
 * storage.  Stores all data in dataFile, and sequence -> data
 * mappings in index.
 *
 * @param uberpair UberStoreFilePair for delegating uberstore operations
 */
class UberStore private[uberstore] (baseDir: String, uberpair: UberPair) extends SiriusLog {

  /**
   * @inheritdoc
   */
  def writeEntry(event: OrderedEvent) {
    uberpair.writeEntry(event)
  }

  /**
   * @inheritdoc
   */
  def getNextSeq = uberpair.getNextSeq

  /**
   * @inheritdoc
   */
  def foldLeftRange[T](startSeq: Long, endSeq: Long)(acc0: T)(foldFun: (T, OrderedEvent) => T): T =
    uberpair.foldLeftRange(startSeq, endSeq)(acc0)(foldFun)

  /**
   * Close underlying file handles or connections.  This UberStore should not be used after
   * close is called.
   */
  def close() {
    uberpair.close()
  }

  /**
   * Consider this closed if either of its underlying objects are closed, no good writes
   * will be able to go through in that case.
   *
   * @return whether this is "closed," i.e., unable to be written to
   */
  def isClosed = uberpair.isClosed

  def compact() {
    // do nothing, don't use compact() on legacy uberstore
  }

  /**
   * sum of size in bytes of all files in uberstore and subdirs
   * @return a measure of size of the SiriusLog
   */
  def size = {
    def recursiveListFiles(f: File): Array[File] = {
           val these = f.listFiles
            these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
    }

    val baseDirFile = new File(baseDir)
    recursiveListFiles(baseDirFile).filter(_.isFile).map(_.length).sum
  }
}
