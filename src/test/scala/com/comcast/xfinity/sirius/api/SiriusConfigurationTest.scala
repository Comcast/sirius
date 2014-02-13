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
  }

  it ("must return data directly, or a default if provided") {
    val underTest = new SiriusConfiguration()

    assert("bar" === underTest.getProp("hello", "bar"))
    underTest.setProp("hello", "world")
    assert("world" === underTest.getProp("hello", "not-world"))
  }
}
