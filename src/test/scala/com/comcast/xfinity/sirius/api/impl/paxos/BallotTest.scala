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

class BallotTest extends NiceTest {

  describe("An empty Ballot") {
    it ("must be really small") {
      assertResult(Ballot(Int.MinValue, "")) {
        Ballot.empty
      }
    }
  }

  describe("A Ballot") {
    it ("must compare equal to an equivalent Ballot") {
      assertResult(true) {
        Ballot(1, "a") == Ballot(1, "a")
      }
    }

    it ("must be less than a Ballot with a greater seq, regardless of leaderId") {
      assertResult(true) {
        Ballot(1, "z") < Ballot(2, "a")
      }
    }

    it ("must be greater than a ballot with a lesser seq, regardless of leaderId") {
      assertResult(true) {
        Ballot(2, "a") > Ballot(1, "z")
      }
    }

    it ("must be less than a Ballot with equal seq but greater leaderId") {
      assertResult(true) {
        Ballot(1, "a") < Ballot(1, "z")
      }
    }

    it ("must be greater than a Ballot with equal seq but lesser leaderId") {
      assertResult(true) {
        Ballot(1, "z") > Ballot(1, "a")
      }
    }

  }

}
