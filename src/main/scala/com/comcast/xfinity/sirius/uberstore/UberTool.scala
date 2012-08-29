package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog

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

}