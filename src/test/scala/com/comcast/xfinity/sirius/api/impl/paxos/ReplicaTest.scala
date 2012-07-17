package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import akka.actor.{ ActorRef, Props, Actor, ActorSystem }
import akka.agent.Agent
import org.mockito.{ Matchers, Mockito }
import org.mockito.Mockito._
import org.mockito.Matchers._
import akka.testkit.{ TestActorRef, TestProbe }
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.impl.Delete

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
        val replica = TestActorRef(Replica(membership))

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
        val replica = TestActorRef(Replica(membership))

        replica.underlyingActor.lowestUnusedSlotNum = 2
        
        replica ! Decision(1, Command(null, 1, Delete("1")))
        assert(2 == replica.underlyingActor.lowestUnusedSlotNum)
        
        replica ! Decision(2, Command(null, 1, Delete("1")))
        assert(3 === replica.underlyingActor.lowestUnusedSlotNum)

        replica ! Decision(4, Command(null, 2, Delete("2")))
        assert(5 === replica.underlyingActor.lowestUnusedSlotNum)
      }
    }
  }

}