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

import com.comcast.xfinity.sirius.NiceTest

class SiriusConfigurationTest extends NiceTest {

  it ("must be able to reliably store arbitrary data") {
    val underTest = new SiriusConfiguration()

    assert(None === underTest.getProp("hello"))
    underTest.setProp("hello", 1)
    assert(Some(1) === underTest.getProp("hello"))

    underTest.setProp("world", "asdf")
    assert(Some("asdf") === underTest.getProp[String]("world"))

    val testObject = new Object()
    underTest.setProp("foo", testObject)
    assert(Some(testObject) === underTest.getProp("foo"))
  }

  it ("must return data directly, or a default if provided") {
    val underTest = new SiriusConfiguration()

    assert("bar" === underTest.getProp("hello", "bar"))
    underTest.setProp("hello", "world")
    assert("world" === underTest.getProp("hello", "not-world"))
  }

  describe("getDouble") {
    it("must return a double for a string") {
      val underTest = new SiriusConfiguration()
      underTest.setProp("foo", "1.0")
      assert(1.0 === underTest.getDouble("foo", 2.0))
      assert(Some(1.0) === underTest.getDouble("foo"))
    }

    it ("must return a double for an int") {
      val underTest = new SiriusConfiguration()
      underTest.setProp("foo", 1)
      assert(1.0 === underTest.getDouble("foo", 2.0))
      assert(Some(1.0) === underTest.getDouble("foo"))
    }

    it ("must return a double for a double") {
      val underTest = new SiriusConfiguration()
      underTest.setProp("foo", 1.0)
      assert(1.0 === underTest.getDouble("foo", 2.0))
      assert(Some(1.0) === underTest.getDouble("foo"))
    }

    it ("must throw an exception if the value can't be turned into a double") {
      val underTest = new SiriusConfiguration()
      underTest.setProp("foo", List(1L, 2L))
      intercept[IllegalArgumentException] {
        underTest.getDouble("foo", 2.0)
      }
    }
  }

  describe("when defaulting / typing with an Int") {
    it("must return an Int for a string") {
      val underTest = new SiriusConfiguration()
      underTest.setProp("foo", "1")
      assert(1 === underTest.getInt("foo", 2))
      assert(Some(1) === underTest.getInt("foo"))
    }

    it ("must return an Int for a Long") {
      val underTest = new SiriusConfiguration()
      underTest.setProp("foo", 1L)
      assert(1 === underTest.getInt("foo", 2))
      assert(Some(1) === underTest.getInt("foo"))
    }

    it ("must return an Int for an Int") {
      val underTest = new SiriusConfiguration()
      underTest.setProp("foo", 1)
      assert(1 === underTest.getInt("foo", 2))
      assert(Some(1) === underTest.getInt("foo"))
    }

    it ("must throw an exception if the value can't be turned into an Int") {
      val underTest = new SiriusConfiguration()
      underTest.setProp("foo", List(1L, 2L))
      intercept[IllegalArgumentException] {
        underTest.getInt("foo", 2)
      }
    }
  }
}
