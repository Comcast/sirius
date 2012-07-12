package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import io.Source

class CloseableSiriusLineIteratorTest extends NiceTest with BeforeAndAfterAll {
  val fileName = "src/test/resources/fakeLogFile.txt"
  var iterator: CloseableSiriusLineIterator = _
  before {
    iterator = new CloseableSiriusLineIterator(fileName)
  }
  after {
    iterator.close()
  }

  describe("a CloseableSiriusLineIterator") {
    it("should pull a line of data from a file, and it should include the line terminator") {
      val source = Source.fromFile(fileName)
      val lines = source.getLines()

      while(iterator.hasNext) {
        assert(iterator.next() === lines.next()+"\n")
      }
    }
    it("should properly report hasNext") {
      val source = Source.fromFile(fileName)
      val lines = source.getLines()

      while(lines.hasNext) {
        assert(lines.hasNext == iterator.hasNext)
        lines.next()
        iterator.next()
      }
      assert(lines.hasNext == iterator.hasNext)
    }
    it("should fail if asked to read after closing") {
      intercept[java.io.IOException] {
        iterator.close()
        iterator.next()
      }
    }
  }
}
