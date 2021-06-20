/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.api

import org.junit.runner.RunWith
import com.comcast.xfinity.sirius.NiceTest
import org.scalatestplus.junit.JUnitRunner

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
        val theThrowable = new Throwable()
        try {
          SiriusResult.exception(theThrowable).getValue
          assert(false, "Exception should have been thrown")
        } catch {
          case t: Throwable => assert(theThrowable === t)
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

      it("should throw an IllegalStateException when it has no exception") {
        intercept[IllegalStateException] {
          SiriusResult.none().getException
        }
      }
      
      it("should return the exception when it has been set") {
        assertResult(true) {
          val t = new Throwable()
          SiriusResult.exception(t).getException == t
        }
      }      
    }
  }
}
