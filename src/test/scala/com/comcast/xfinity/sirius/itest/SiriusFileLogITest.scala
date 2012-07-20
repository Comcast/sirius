package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.writeaheadlog.{RangedSiriusEventIterator, CloseableSiriusEventIterator, WALSerDe, SiriusFileLog}
import com.comcast.xfinity.sirius.api.impl.persistence.{BoundedLogRange, EntireLog}

class SiriusFileLogITest extends NiceTest {

  var log: SiriusFileLog = _
  var mockSerDe: WALSerDe = _
  val filename = "src/test/resources/fakeLogFile.txt"

  before {
    mockSerDe = mock[WALSerDe]
    log = new SiriusFileLog(filename, mockSerDe)
  }

  describe("SiriusFileLog") {

    describe(".createIterator") {
      it("should create an iterator that traverses the entire file") {
        val iterator = log.createIterator(EntireLog)

        assert(iterator.getClass === classOf[CloseableSiriusEventIterator])
        val castedIterator = iterator.asInstanceOf[CloseableSiriusEventIterator]

        assert(castedIterator.serDe === mockSerDe)
        assert(castedIterator.filePath === filename)
      }
    }


    describe(".createRangedIterator") {
      it("should create an iterator that traverses the specified range of events") {
        val startRange = 52
        val endRange = 992
        val iterator = log.createIterator(new BoundedLogRange(startRange, endRange))

        assert(iterator.getClass === classOf[RangedSiriusEventIterator])
        val castedIterator = iterator.asInstanceOf[RangedSiriusEventIterator]

        assert(castedIterator.serDe === mockSerDe)
        assert(castedIterator.filePath === filename)
        assert(castedIterator.startRange === startRange)
        assert(castedIterator.endRange === endRange)
      }
    }
  }
}
