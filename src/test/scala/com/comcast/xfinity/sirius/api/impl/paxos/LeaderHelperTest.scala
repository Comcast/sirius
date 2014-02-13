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
package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.util.RichJTreeMap

class LeaderHelperTest extends NiceTest {

  val leaderHelper = new LeaderHelper()

  describe("LeaderHelper") {

    describe("update") {
      it ("must return a reference to x, with all of the values from y overlaid on top of it") {
        val x = RichJTreeMap(1L -> 5, 2L -> 3)

        val y = RichJTreeMap(1L -> 2, 3L -> 4)

        assert(x === leaderHelper.update(x, y))
        assert(3 === x.size)
        assert(2 === x.get(1L))
        assert(3 === x.get(2L))
        assert(4 === x.get(3L))
      }
    }

    describe("pmax") {
      it ("must, for each unique slotNum in a Set[PValue], return the Map where keys are the slotNum, and the values " +
          "are those associated with the highest ballotNum for that given slotNum") {
        val pvals = Set(
          PValue(Ballot(1, "a"), 1, Command(null, 1234, Delete("1"))),
          PValue(Ballot(2, "a"), 1, Command(null, 12345, Delete("2"))),
          PValue(Ballot(1, "a"), 2, Command(null, 123, Delete("3")))
        )

        val expected = RichJTreeMap(
          1L -> Command(null, 12345, Delete("2")),
          2L -> Command(null, 123, Delete("3"))
        )

        assert(expected === leaderHelper.pmax(pvals))
      }
    }
  }
}
