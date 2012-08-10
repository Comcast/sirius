package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import akka.actor.{ ActorRef, ActorSystem }
import akka.agent.Agent
import akka.testkit.{ TestActorRef, TestProbe }
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.impl.{Put, NonCommutativeSiriusRequest, Delete}

class ReplicaTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("ReplicaTest")

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("A Replica") {
    describe("when receiving a Request message") {
      it("must choose a slot number, send a Propose message to all leaders, update its lowest unused slot and" +
         "store the proposal") {
        val memberProbes = Set(TestProbe(), TestProbe(), TestProbe())
        val membership = Agent(memberProbes.map(_.ref))
        val replica = TestActorRef(new Replica(membership, 1, d => ()))

        val command = Command(null, 1, Delete("1"))

        replica ! Request(command)
        memberProbes.foreach(_.expectMsg(Propose(1, command)))
        assert(2 === replica.underlyingActor.lowestUnusedSlotNum)
      }
    }

    describe("when receiving a Decision message") {
      it("must update its lowest unused slot number iff the decision is greater than or equal to the " +
         "current unused slot") {
        val membership = Agent(Set[ActorRef]())
        val replica = TestActorRef(new Replica(membership, 2, d => ()))

        replica ! Decision(1, Command(null, 1, Delete("1")))
        assert(2 == replica.underlyingActor.lowestUnusedSlotNum)
        
        replica ! Decision(2, Command(null, 1, Delete("1")))
        assert(3 === replica.underlyingActor.lowestUnusedSlotNum)

        replica ! Decision(4, Command(null, 2, Delete("2")))
        assert(5 === replica.underlyingActor.lowestUnusedSlotNum)
      }

      it("must pass the decision to the delegated function for handling") {
        val membership = Agent(Set[ActorRef]())
        var appliedDecisions = Set[Decision]()
        val replica = TestActorRef(
          new Replica(membership, 1,
                      d => appliedDecisions += d
          )
        )

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

      it("must not let an exception thrown by performFun ruin its day") {
        val membership = Agent(Set[ActorRef]())

        val wasRestartedProbe = TestProbe()
        // TestActorRef so message handling is dispatched on the same thread
        val replica = TestActorRef(
          new Replica(membership, 1,
            d => throw new RuntimeException("The dishes are done man")
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
        assert(2 === replica.underlyingActor.lowestUnusedSlotNum)

        // make sure we didn't crash
        wasRestartedProbe.expectNoMsg()

        // and our state is still cool
        replica ! Decision(2, Command(null, 1, Delete("1234")))
        assert(3 === replica.underlyingActor.lowestUnusedSlotNum)

        // and do it again
        wasRestartedProbe.expectNoMsg()
      }
    }
  }

}