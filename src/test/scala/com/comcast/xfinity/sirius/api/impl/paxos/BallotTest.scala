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