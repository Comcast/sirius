package com.comcast.xfinity.sirius.api

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.NiceTest

@RunWith(classOf[JUnitRunner])
class SiriusResultTest extends NiceTest {
  
  describe("SiriusResult") {
    it("should return it's value when it has a value") {
      val body = "hello".getBytes
      expect(body) {
        SiriusResult.some(body).getValue
      }
    }
    
    it("should throw an IllegalStateException when it has no value") {
      intercept[IllegalStateException] {
        SiriusResult.none().getValue
      }
    }
    
    it("should indicate when it has a value") {
      expect(true) {
        SiriusResult.some("hello".getBytes).hasValue
      }
    }
    
    it("should indicate when it has no value") {
      expect(false) {
        SiriusResult.none().hasValue
      }
    }
    
  }
}