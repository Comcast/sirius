package com.comcast.xfinity.sirius.api
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec

@RunWith(classOf[JUnitRunner])
class AbstractRequestHandlerTest extends FunSpec with BeforeAndAfter {
  
  class FauxAbstractRequestHandler extends AbstractRequestHandler {
    def doPut(key: String, body: Array[Byte]) = null
    def doGet(key: String, body: Array[Byte]) = null
    def doDelete(key: String, body: Array[Byte]) = null    
  }
  
  var underTest: AbstractRequestHandler = _
  
  before {
    underTest = Mockito.spy(new FauxAbstractRequestHandler())
  }
  
  describe("an AbstractRequestHandler") {
    it("should forward GETs to doGet") {
      val key = "key"
      val body = "val".getBytes()
      underTest.handle(RequestMethod.GET, key, body)
      Mockito.verify(underTest).doGet(key, body)
    }
    
    it("should forward PUTs to doPut") {
      val key = "key"
      val body = "val".getBytes()
      underTest.handle(RequestMethod.PUT, key, body)
      Mockito.verify(underTest).doPut(key, body)
    }
    
    it("should forward DELETEs to doDelete") {
      val key = "key"
      val body = "val".getBytes()
      underTest.handle(RequestMethod.DELETE, key, body)
      Mockito.verify(underTest).doDelete(key, body)
    }
  }
  
}