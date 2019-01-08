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

import org.scalatest.BeforeAndAfterAll

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.Put
import com.comcast.xfinity.sirius.NiceTest

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.agent.Agent
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import scala.collection.JavaConversions._
import collection.SortedMap
import com.comcast.xfinity.sirius.api.impl.paxos.Replica.Reap
import com.comcast.xfinity.sirius.api.SiriusConfiguration

class ReplicaTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("ReplicaTest")

  override def afterAll() {
    actorSystem.terminate()
  }

  def makeReplica(localLeader: ActorRef = TestProbe().ref,
                  startingSlot: Long = 1,
                  performFun: Replica.PerformFun = d => (),
                  reproposalWindowSecs: Int = 5,
                  reapFreqSecs: Int = 1) = {
    TestActorRef(new Replica(localLeader, startingSlot, performFun, reproposalWindowSecs, reapFreqSecs, new SiriusConfiguration))
  }
  
  describe("A Replica") {
    describe("when receiving a Request message") {
      it("must choose a slot number, send a Propose message to its local leader, update its lowest unused slot and" +
         "store the proposal") {
        val localLeader = TestProbe()
        val replica = makeReplica(localLeader.ref)
        val command = Command(null, 1, Delete("1"))

        replica ! Request(command)
        localLeader.expectMsg(Propose(1, command))
        assert(2 === replica.underlyingActor.nextAvailableSlotNum)
      }
    }

    describe("when receiving a Decision message") {
      it("must update its lowest unused slot number iff the decision is greater than or equal to the " +
         "current unused slot") {
        val localLeader = TestProbe()
        val replica = makeReplica(localLeader.ref, 2)

        replica ! Decision(1, Command(null, 1, Delete("1")))
        assert(2 == replica.underlyingActor.nextAvailableSlotNum)
        
        replica ! Decision(2, Command(null, 1, Delete("1")))
        assert(3 === replica.underlyingActor.nextAvailableSlotNum)

        replica ! Decision(4, Command(null, 2, Delete("2")))
        assert(3 === replica.underlyingActor.nextAvailableSlotNum)
      }
      

      it("must pass the decision to the delegated function for handling") {
        var appliedDecisions = Set[Decision]()
        val replica = makeReplica(performFun = d => appliedDecisions += d)

        val requester1 = TestProbe()
        val request1 = Delete("1")
        val decision1 = Decision(1, Command(requester1.ref, 1, request1))
        replica ! decision1
        assert(Set(decision1) === appliedDecisions)

        val requester2 = TestProbe()
        val request2 = Put("asdf", "1234".getBytes)
        val decision2 = Decision(2, Command(requester2.ref, 1, request2))
        replica ! decision2
        assert(Set(decision1, decision2) === appliedDecisions)
      }

      it("must notify the originator of the request if performFun function says so") {
        val localLeader = TestProbe()

        val wasRestartedProbe = TestProbe()
        // TestActorRef so message handling is dispatched on the same thread
        val replica = TestActorRef(
          new Replica(localLeader.ref, 1,
            d => throw new RuntimeException("The dishes are done man"), 5, 1,
            new SiriusConfiguration
          ) {
            // this is weird, if the actor terminates, it is restarted
            //  asynchronously, so we need to propogate the failure in
            //  some other way
            override def preRestart(e: Throwable, m: Option[Any]) {
              wasRestartedProbe.ref ! 'brokenaxle
            }
          }
        )

        replica ! Decision(1, Command(null, 1, Delete("asdf")))
        assert(2 === replica.underlyingActor.nextAvailableSlotNum)

        // make sure we didn't crash
        wasRestartedProbe.expectNoMsg()

        // and our state is still cool
        replica ! Decision(2, Command(null, 1, Delete("1234")))
        assert(3 === replica.underlyingActor.nextAvailableSlotNum)

        // and do it again
        wasRestartedProbe.expectNoMsg()
      }

      it("must repropose a command if a different decision using the command's " +
        "proposed slot number arrives, updating outstanding proposals") {
        val localLeader = TestProbe()
        val replica = makeReplica(localLeader.ref, 2)

        val commandToBeReproposed = Command(null, 1, Delete("ThisThing"))
        replica ! Request(commandToBeReproposed)
        localLeader.expectMsg(Propose(2, commandToBeReproposed))
        assert(replica.underlyingActor.outstandingProposals.size == 1)
        assert(replica.underlyingActor.outstandingProposals.containsKey(2L))

        replica ! Decision(2, Command(null, 1, Delete("ADifferntThing")))
        localLeader.expectMsg(Propose(3, commandToBeReproposed))

        assert(1 === replica.underlyingActor.outstandingProposals.size)
        assert(commandToBeReproposed === replica.underlyingActor.outstandingProposals.get(3L))
      }

      it("should handle a decision for a non-proposed slot number with no side-effects") {
        val localLeader = TestProbe()
        val replica = makeReplica(localLeader.ref, 2)

        replica ! Request(Command(null, 1, Delete("ThisThing")))
        assert(1 === replica.underlyingActor.outstandingProposals.size)
        localLeader.expectMsg(Propose(2, Command(null, 1, Delete("ThisThing"))))

        replica ! Decision(1, Command(null, 1, Delete("ThisOtherThing")))
        assert(1 === replica.underlyingActor.outstandingProposals.size)
        localLeader.expectNoMsg()
      }
    }

    describe ("in response to a DecisionHint") {
      it("ignores the DH if it's out of date") {
        val localLeader = TestProbe()
        val replica = makeReplica(localLeader.ref, 5)
        replica ! DecisionHint(2)

        assert(5 === replica.underlyingActor.slotNum)
        localLeader.expectNoMsg()
      }
      it("updates the slotNum") {
        val localLeader = TestProbe()
        val replica = makeReplica(localLeader.ref, 2)

        replica ! DecisionHint(3)
        assert(4 === replica.underlyingActor.slotNum)
      }

      it("sends its local leader the decision hint") {
        val localLeader = TestProbe()
        val replica = makeReplica(localLeader.ref, 2)

        replica ! DecisionHint(2)
        localLeader.expectMsg(DecisionHint(2))
      }

      it("must prune proposals when matching decision hint arrives") {
        val localLeader = TestProbe()
        val replica = makeReplica(localLeader.ref, 2)

        replica ! Request(Command(null, 1, Delete("ThisThing")))
        assert(replica.underlyingActor.outstandingProposals.size == 1)

        replica ! DecisionHint(2)
        assert(replica.underlyingActor.outstandingProposals.isEmpty)
      }

      it("must prune decisions when matching decision hint arrives") {
        val replica = makeReplica(startingSlot = 2)

        replica ! Decision(2, Command(null, 1, Delete("ThisThing")))
        assert(replica.underlyingActor.decisions.size == 1)

        replica ! DecisionHint(2)
        assert(replica.underlyingActor.decisions.isEmpty)
      }

      it("must not prune proposals too much when a decision hint arrives") {
        val replica = makeReplica(startingSlot = 2)

        replica ! Decision(2, Command(null, 1, Delete("ThisThing")))
        replica ! Decision(3, Command(null, 1, Delete("ThatThing")))
        assert(replica.underlyingActor.decisions.size == 2)

        replica ! DecisionHint(2)
        assert(1 === replica.underlyingActor.decisions.size)
      }
    }

    describe("in response to a Reap message") {
      it ("must truncate the proposals that are out of date, regardless of slot") {
        val replica = makeReplica(startingSlot = 2)

        val now = System.currentTimeMillis()
        replica.underlyingActor.outstandingProposals.putAll(SortedMap[Long, Command](
          1L -> Command(null, now - 15000, Delete("1")),
          2L -> Command(null, now, Delete("2")),
          3L -> Command(null, now - 12000, Delete("3"))
        ))

        replica ! Reap

        assert(1 === replica.underlyingActor.outstandingProposals.size)
        assert(replica.underlyingActor.outstandingProposals.containsKey(2L))
      }
    }
  }
}
