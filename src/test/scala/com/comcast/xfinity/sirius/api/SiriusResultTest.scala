package com.comcast.xfinity.sirius.api

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.NiceTest

@RunWith(classOf[JUnitRunner])
class SiriusResultTest extends NiceTest {
  
  describe("SiriusResult") {
    
    describe(".hasValue") {
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
    
    describe(".getValue") {
      it("should return it's value when it has a value") {
        // XXX: due to how Arrays work, as inherited from Java,
        //      we must use the same reference
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
    }
    
    describe(".equals") {
      it("should return true if both have no value") {
        expect(true) {
          SiriusResult.none().equals(SiriusResult.none())
        }
      }
      
      it("should return true if both values exist and are equal") {
        expect(true) {
          SiriusResult.some("asdf".getBytes).equals(SiriusResult.some("asdf".getBytes))
        }
      }
      
      it("should return false if both values exist and differ") {
        expect(false) {
          SiriusResult.some("asdf".getBytes).equals(SiriusResult.some("dsfa".getBytes))
        }
      }
      
      it("should return false when the lhs value exists and rhs does not") {
        expect(false) {
          SiriusResult.some("asdf".getBytes).equals(SiriusResult.none())
        }
      }
      
      it("should return false when the lhs value does not exist and the rhs does") {
        expect(false) {
          SiriusResult.none().equals(SiriusResult.some("asdf".getBytes))
        }
      }
    }
    
  }
}