package com.comcast.xfinity.sirius.api

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.NiceTest

@RunWith(classOf[JUnitRunner])
class SiriusResultTest extends NiceTest {
  
  describe("SiriusResult") {
    
    describe(".hasValue") {
      it("should indicate when it has a value when it does") {
        assertResult(true) {
          SiriusResult.some("hello").hasValue
        }
      }

      it("should indicate that it has a value when it has an exception") {
        assertResult(true) {
          SiriusResult.error(new RuntimeException()).hasValue
        }
      }
    
      it("should indicate when it has no value") {
        assertResult(false) {
          SiriusResult.none().hasValue
        }
      }
    }
    
    describe(".getValue") {
      it("should return it's value when it has a value") {
        val body = "hello"
        assertResult(body) {
          SiriusResult.some(body).getValue
        }
      }
    
      it("should throw an IllegalStateException when it has no value") {
        intercept[IllegalStateException] {
          SiriusResult.none().getValue
        }
      }
      
      it("should rethrow the exception when it has an error") {
        val theException = new RuntimeException()
        try {
          SiriusResult.error(theException).getValue
          assert(false, "Exception should have been thrown")
        } catch {
          case rte: RuntimeException => assert(theException === rte)
        }
      }      
    }

    describe(".isError") {
      it("should return false when it has a value") {
        assertResult(false) {
          SiriusResult.some("value").isError
        }
      }

      it("should return false when it has no value") {
        assertResult(false) {
          SiriusResult.none().isError
        }
      }

      it("should return true when there is a wrapped exception") {
        assertResult(true) {
          SiriusResult.error(new RuntimeException()).isError
        }
      }
    }
  }
}