package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.writeaheadlog.{LogDataSerDe, SiriusFileLog}
import io.Source

class SiriusLogFileITest extends NiceTest {

  var siriusLog: SiriusFileLog = _
  var logDataSerDe: LogDataSerDe = _
  val logFileName = "src/test/resources/fakeLogFile.txt"

  before {
    logDataSerDe = mock[LogDataSerDe]
    siriusLog = new SiriusFileLog(logFileName, logDataSerDe)
  }

  describe("a SiriusLogFile") {
    it("should return an iterator of the lines of a file when createLinesIterator() is called") {
      val expectedIter = Source.fromFile(logFileName).getLines()
      val actualIter = siriusLog.createLinesIterator()

      while(expectedIter.hasNext) {
        assert(expectedIter.next() === actualIter.next())
      }
    }
  }

}
