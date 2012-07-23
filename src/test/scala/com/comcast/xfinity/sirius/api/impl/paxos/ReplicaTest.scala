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
        val replica = TestActorRef(Replica(membership, (s, r) => ()))

        val command = Command(null, 1, Delete("1"))
        replica.underlyingActor.lowestUnusedSlotNum = 1

        replica ! Request(command)
        memberProbes.foreach(_.expectMsg(Propose(1, command)))
        assert(2 === replica.underlyingActor.lowestUnusedSlotNum)
      }
    }

    describe("when receiving a Decision message") {
      it("must update its lowest unused slot number iff the decision is greater than or equal to the current unused slot") {
        val membership = Agent(Set[ActorRef]())
        val replica = TestActorRef(Replica(membership, (s, r) => ()))

        replica.underlyingActor.lowestUnusedSlotNum = 2
        
        replica ! Decision(1, Command(null, 1, Delete("1")))
        assert(2 == replica.underlyingActor.lowestUnusedSlotNum)
        
        replica ! Decision(2, Command(null, 1, Delete("1")))
        assert(3 === replica.underlyingActor.lowestUnusedSlotNum)

        replica ! Decision(4, Command(null, 2, Delete("2")))
        assert(5 === replica.underlyingActor.lowestUnusedSlotNum)
      }

      it("must pass the decision and slot number to the delegated function") {
        val membership = Agent(Set[ActorRef]())
        var appliedDecisions = Set[(Long, NonCommutativeSiriusRequest)]()
        val replica = TestActorRef(Replica(membership,
          (s, r) => appliedDecisions += Tuple2(s, r)
        ))

        val request1 = Delete("1")
        val request2 = Put("asdf", "1234".getBytes)

        replica ! Decision(1, Command(null, 1, request1))
        assert(Set((1L, request1)) === appliedDecisions)

        replica ! Decision(1, Command(null, 1, request1))
      }
    }
  }

}