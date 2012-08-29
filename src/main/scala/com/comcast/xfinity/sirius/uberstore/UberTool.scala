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

  private def keyFromEvent(evt: OrderedEvent): String = evt.request match {
    case Put(key, _) => key
    case Delete(key) => key
  }
}