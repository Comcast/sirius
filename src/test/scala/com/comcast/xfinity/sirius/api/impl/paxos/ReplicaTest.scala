package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.{Props, Actor, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.{Slot, Command}

class ReplicaTest extends NiceTest {
  import Replica._

  describe("ReplicaActorTest") {
    describe("decisionExistsForCommand") {
      it ("must return true if a decision exists") {
        expect(true) {
          val decisions = Set(
            Slot(1, Command(null, 1, 1)),
            Slot(2, Command(null, 2, 2))
          )
          decisionExistsForCommand(decisions, Command(null, 2, 2))
        }
      }

      it ("must return false if no such decision exists") {
        expect(false) {
          val decisions = Set(
            Slot(1, Command(null, 1, 1)),
            Slot(2, Command(null, 2, 2))
          )
          decisionExistsForCommand(decisions, Command(null, 100, 2))
        }
      }
    }

    describe("getLowestUnusedSlotNum") {
      it ("must return 1 if no slot numbers exist") {
        expect(1) {
          getLowestUnusedSlotNum(Set[Slot]())
        }
      }

      it ("must return the lowest unused slot number") {
        expect(3) {
          val slots = Set(
            Slot(1, Command(null, 1, 1)),
            Slot(2, Command(null, 2, 2))
          )
          getLowestUnusedSlotNum(slots)
        }
      }
    }

    describe("getUnperformedDecisions") {
      it ("must return an empty list if no decisions are queued") {
        expect(Nil) {
          getUnperformedDecisions(Set[Slot](), 1)
        }
      }

      it ("must return an empty list if there exists no decision at the current slot") {
        expect(Nil) {
          val slots = Set(
            Slot(4, null),
            Slot(3, null)
          )
          getUnperformedDecisions(slots, 1)
        }
      }

      it ("must return only the decisions ready for execution") {
        expect(List(Slot(2, null), Slot(3, null))) {
          val slots = Set(
            Slot(2, null),
            Slot(3, null),
            Slot(5, null)
          )
          getUnperformedDecisions(slots, 1)
        }
      }
    }
  }

}