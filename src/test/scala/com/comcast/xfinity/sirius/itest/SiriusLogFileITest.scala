package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.writeaheadlog.{WALSerDe, SiriusFileLog}
import io.Source
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import org.scalatest.Ignore

class SiriusLogFileITest extends NiceTest {

  var siriusLog: SiriusFileLog = _
  var logDataSerDe: WALSerDe = _
  val logFileName = "src/test/resources/fakeLogFile.txt"

  before {
    logDataSerDe = mock[WALSerDe]
    siriusLog = new SiriusFileLog(logFileName, logDataSerDe)
  }

  // ignoring for now, think we sort of test this somewhere else actually...
  describe("a SiriusLogFile") {
    ignore("should return an iterator of the events of a file when createIterator() is called") {
      val file = Path.fromString(logFileName)
      val expectedIter = file.lines(NewLine, true).toIterator
      val actualIter = siriusLog.createIterator()

      while(expectedIter.hasNext) {
        assert(expectedIter.next() === actualIter.next())
      }
    }
  }

}
