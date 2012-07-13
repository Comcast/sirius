package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import io.Source
import org.mockito.Mockito

class CloseableSiriusEventIteratorTest extends NiceTest with BeforeAndAfterAll {
  val fileName = "src/test/resources/fakeLogFile.txt"
  var iterator: CloseableSiriusEventIterator = _
  var mockSerDe = mock[WALSerDe]
  before {
    iterator = new CloseableSiriusEventIterator(fileName, mockSerDe)
  }
  after {
    iterator.close()
  }

  describe("a CloseableSiriusEventIterator") {
    it("should pull a line of data from a file and try to deserialize it") {
      val source = Source.fromFile(fileName)
      val lines = source.getLines()

      // XXX: will this work?
      while(iterator.hasNext) {
        iterator.next()
        Mockito.verify(mockSerDe).deserialize(lines.next()+"\n")
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
