package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Slot._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Command._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.{Command, Slot}

class ReplicaHelperTest extends NiceTest {

  val replicaHelper = new ReplicaHelper()

  describe("A ReplicaHelper") {

    describe("for decisionExistsForCommand") {
      it ("must return true if a decision exists") {
        expect(true) {
          val decisions = Set(
            Slot(1, Command(null, 1, 1)),
            Slot(2, Command(null, 2, 2))
          )
          replicaHelper.decisionExistsForCommand(decisions, Command(null, 2, 2))
        }
      }

      it ("must return false if no such decision exists") {
        expect(false) {
          val decisions = Set(
            Slot(1, Command(null, 1, 1)),
            Slot(2, Command(null, 2, 2))
          )
          replicaHelper.decisionExistsForCommand(decisions, Command(null, 100, 2))
        }
      }
    }

    describe("for getLowestUnusedSlotNum") {
      it ("must return 1 if no slot numbers exist") {
        expect(1) {
          replicaHelper.getLowestUnusedSlotNum(Set[Slot]())
        }
      }

      it ("must return the lowest unused slot number") {
        expect(3) {
          val slots = Set(
            Slot(1, Command(null, 1, 1)),
            Slot(2, Command(null, 2, 2))
          )
          replicaHelper.getLowestUnusedSlotNum(slots)
        }
      }
    }

    describe("for getUnperformedDecisions") {
      it ("must return an empty list if no decisions are queued") {
        expect(Nil) {
          replicaHelper.getUnperformedDecisions(Set[Slot](), 1)
        }
      }

      it ("must return an empty list if there exists no decision at the current slot") {
        expect(Nil) {
          val slots = Set(
            Slot(4, null),
            Slot(3, null)
          )
          replicaHelper.getUnperformedDecisions(slots, 1)
        }
      }

      it ("must return only the decisions ready for execution") {
        expect(List(Slot(2, null), Slot(3, null))) {
          val slots = Set(
            Slot(2, null),
            Slot(3, null),
            Slot(5, null)
          )
          replicaHelper.getUnperformedDecisions(slots, 1)
        }
      }
    }
  }
}