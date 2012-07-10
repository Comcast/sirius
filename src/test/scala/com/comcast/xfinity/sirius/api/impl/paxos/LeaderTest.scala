package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

class LeaderTest extends NiceTest {

  import Leader._

  describe("LeaderActor") {
    describe("proposalExistsForSlot") {
      it ("must return false if no proposal exists for the slot") {
        expect(false) {
          val slots = Set(
            Slot(1, Command(null, 1, 1)),
            Slot(2, Command(null, 2, 2))
          )
          proposalExistsForSlot(slots, 3)
        }
      }

      it ("must return true if a proposal exists for the slot") {
        expect(true) {
          val slots = Set(
            Slot(1, Command(null, 1, 1)),
            Slot(2, Command(null, 2, 2))
          )
          proposalExistsForSlot(slots, 2)
        }
      }
    }

    describe("update") {
      it ("must return the set of all items in y and all items in x not in y") {
        expect(Set(1, 2, 3, 4)) {
          update(Set(1, 2), Set(3, 4))
        }
      }
    }

    describe("pmax") {
      it("must, for each unique slotNum in a Set[PValue], return the Slot(slotNum, proposal) associated with the highest" +
         "ballotNum for that given slotNum") {
        expect(Set(Slot(1, Command(null, 12345, 2)), Slot(2, Command(null, 123, 3)))) {
          val pvals = Set(
            PValue(Ballot(1, "a"), 1, Command(null, 1234, 1)),
            PValue(Ballot(2, "a"), 1, Command(null, 12345, 2)),
            PValue(Ballot(1, "a"), 2, Command(null, 123, 3))
          )
          pmax(pvals)
        }
      }
    }
  }
}