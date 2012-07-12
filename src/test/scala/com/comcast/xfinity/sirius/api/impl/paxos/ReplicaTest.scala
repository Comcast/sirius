package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import akka.actor.{ActorRef, Props, Actor, ActorSystem}
import akka.agent.Agent
import org.mockito.{Matchers, Mockito}
import org.mockito.Mockito._
import org.mockito.Matchers._
import akka.testkit.{TestActorRef, TestProbe}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._

object ReplicaTest {
  def makeReplicaWithMockHelper(membership: Agent[Set[ActorRef]])(implicit as: ActorSystem) = {
    val mockHelper = mock(classOf[ReplicaHelper])
    val replica = TestActorRef(
        new Replica(membership) with Replica.HelperProvider {
          val replicaHelper = mockHelper
        })
    (replica, mockHelper)
  }
}

class ReplicaTest extends NiceTest with BeforeAndAfterAll {

  import ReplicaTest._

  implicit val actorSystem = ActorSystem("ReplicaTest")

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("A Replica") {
    describe("when receiving a Request message") {
      it ("must choose a slot number and send a Propose message to all leaders if " +
          "no such Request has already been made") {
        val memberProbes = Set(TestProbe(), TestProbe(), TestProbe())
        val membership = Agent(memberProbes.map(_.ref))
        val (replica, mockHelper) = makeReplicaWithMockHelper(membership)

        doReturn(false).when(mockHelper).
          decisionExistsForCommand(any(classOf[Set[Slot]]), any(classOf[Command]))
        doReturn(2).when(mockHelper).
          getLowestUnusedSlotNum(any[Set[Slot]])

        val command = Command(null, 1, 1)
        replica ! Request(command)

        verify(mockHelper).decisionExistsForCommand(Set[Slot](), command)

        memberProbes.foreach(_.expectMsg(Propose(2, command)))
        assert(Set(Slot(2, command)) === replica.underlyingActor.proposals)
      }

      it ("must ignore the Request if the command already exists") {
        val memberProbe = TestProbe()
        val membership = Agent(Set(memberProbe.ref))
        val (replica, mockHelper) = makeReplicaWithMockHelper(membership)

        doReturn(true).when(mockHelper).
          decisionExistsForCommand(any(classOf[Set[Slot]]), any(classOf[Command]))

        replica ! Request(Command(null, 1, 1))

        memberProbe.expectNoMsg()
      }
    }

    describe("when receiving a Decision message") {
      it ("must add the decision to its state and apply all decisions ready, updating " +
          "the highest performed slot in the process") {
        val membership = Agent(Set[ActorRef]())
        val (replica, mockHelper) = makeReplicaWithMockHelper(membership)

        replica.underlyingActor.highestPerformedSlot = 5
        doReturn(List(Slot(6, Command(null, 1, 2)), Slot(7, Command(null, 5, 6)))).when(mockHelper).
          getUnperformedDecisions(any(classOf[Set[Slot]]), anyInt())

        replica ! Decision(9, Command(null, 0, 9))

        // TODO: verify that perform actually does something...
        assert(7 === replica.underlyingActor.highestPerformedSlot)
        assert(Set(Slot(9, Command(null, 0, 9))) === replica.underlyingActor.decisions)
      }
    }
  }

}