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

package com.comcast.xfinity.sirius.util

import com.comcast.xfinity.sirius.NiceTest

class RichJTreeMapTest extends NiceTest {

  describe ("its companion constructor") {
    it ("must add all elements provided to the constructor") {
      val underTest = RichJTreeMap(1 -> 2, 2 -> 3)

      assert(2 === underTest.size)
      assert(2 === underTest.get(1))
      assert(3 === underTest.get(2))
    }

    it ("must add all elements from a map provided to the constructor") {
      val underTest = RichJTreeMap(Map(1 -> 2, 2 -> 3))

      assert(2 === underTest.size)
      assert(2 === underTest.get(1))
      assert(3 === underTest.get(2))
    }
  }

  it ("must properly implement foreach, applying the passed in function to each kv in order") {
    val underTest = RichJTreeMap(
      "hello" -> "world",
      "why" -> "do the dbas spontaneously change ports?",
      "sincerely" -> "developers"
    )


    var accum = List[(String, String)]()
    underTest.foreach((k, v) => accum = ((k, v) :: accum))

    // Note this is the sorted result of the above inserts
    val expected = List(
      ("hello", "world"),
      ("sincerely", "developers"),
      ("why", "do the dbas spontaneously change ports?")
    )
    assert(expected === accum.reverse)
  }

  it ("must properly implement filter, mutating the underlying collection") {
    val underTest = RichJTreeMap(
      "Well" -> "I'll tell you why",
      "It's" -> "To keep us on our toes"
    )

    underTest.filter((k, v) => k == "It's" && v == "To keep us on our toes")

    assert(1 === underTest.size)
    assert("To keep us on our toes" === underTest.get("It's"))
  }

  it ("must properly implement dropWhile, mutating the underlying collection " +
      "and not doing more work than is necessary") {
    val underTest = RichJTreeMap(
      "A" -> "1",
      "B" -> "2",
      "C" -> "3"
    )

    var lastCheckedKV: Option[(String, String)] = None

    underTest.dropWhile(
      (k, v) => {
        lastCheckedKV = Some((k, v))
        k != "B" && v != "2"
      }
    )

    assert(2 === underTest.size)
    assert("2" === underTest.get("B"))
    assert("3" === underTest.get("C"))
    assert(Some(("B", "2")) === lastCheckedKV)
  }
}
