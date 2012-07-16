package com.comcast.xfinity.sirius.writeaheadlog

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}

class RangedSiriusEventIteratorTest extends NiceTest with BeforeAndAfterAll {
  
  var iterator: RangedSiriusEventIterator = _
  var serDe: WALSerDe = _

  before {
    serDe = new WALSerDe {
      def deserialize(rawData: String): OrderedEvent = {
        val fields = rawData.split("\\|");
        if (fields.length < 4) {
          throw new IllegalArgumentException("Cannot parse sequence number from: " + rawData)
        }
        new OrderedEvent(fields(3).toLong, 0, Delete(""))
      }
      def serialize(event: OrderedEvent): String = "Nothing"
    }
  }
  
  after {
    iterator.close()
  }  
  
  describe("a RangedSiriusEventIterator") {
    it("should find the first line of a range") {
      
      iterator = new RangedSiriusEventIterator("src/test/resources/fakeRangedLogFile.txt", serDe, 5, 14)
      iterator.hasNext
      val event = iterator.next()
      assert(event.sequence === 5)
    }
    
    it("should stop at the last line of a range") {
      
      iterator = new RangedSiriusEventIterator("src/test/resources/fakeRangedLogFile.txt", serDe, 5, 15)
      var event:OrderedEvent = null
      while (iterator.hasNext) {
        event = iterator.next()
      }
      assert(event.sequence === 15)
    }    
    
    it("should stop at the end of the file even if its before the end of the range") {
      
      iterator = new RangedSiriusEventIterator("src/test/resources/fakeRangedLogFile.txt", serDe, 5, 50)
      var event:OrderedEvent = null
      while (iterator.hasNext) {
        event = iterator.next()
      }
      assert(event.sequence === 20)
    }     
    
    it("should stop at the beginning of the file if it begins past endRange") {
      
      iterator = new RangedSiriusEventIterator("src/test/resources/fakeRangedLogFile.txt", serDe, 1, 2)
      assert(iterator.hasNext === false)
    }       
  }
  
}
