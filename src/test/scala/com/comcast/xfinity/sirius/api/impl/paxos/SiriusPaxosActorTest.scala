package com.comcast.xfinity.sirius.api.impl.paxos

import akka.util.duration._

import org.junit.Assert.assertTrue


import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.membership._
import org.mockito.Mockito._
import akka.agent.Agent
import com.comcast.xfinity.sirius.info.SiriusInfo
import com.comcast.xfinity.sirius.api.impl._
import akka.actor.{ActorRef, ActorSystem}

class SiriusPaxosActorTest extends NiceTest {

  var actorSystem: ActorSystem = _
  var underTestActor: TestActorRef[SiriusPaxosActor] = _
  var underTest: SiriusPaxosActor = _
  var persistenceProbe: TestProbe = _
  var mockMembershipAgent: Agent[MembershipMap] = _
  var paxosSupervisorProbe: TestProbe = _


  def createProbedSiriusPaxosActor(paxosSupervisorProbe: TestProbe, persistenceProbe: TestProbe,
                                   membershipAgent: Agent[MembershipMap])(implicit as: ActorSystem) = {
    TestActorRef(new SiriusPaxosActor(persistenceProbe.ref, membershipAgent) {
      override def createPaxosSupervisor(memAgent: Agent[MembershipMap],
                                         perfDec: Replica.PerformFun): ActorRef =
        paxosSupervisorProbe.ref

    })
  }

  before {
    actorSystem = ActorSystem("testsystem")
    persistenceProbe = TestProbe()(actorSystem)
    paxosSupervisorProbe = TestProbe()(actorSystem)
    mockMembershipAgent = mock[Agent[MembershipMap]]
    when(mockMembershipAgent.get()).thenReturn(Map[String, MembershipData]())



    underTestActor = createProbedSiriusPaxosActor(paxosSupervisorProbe, persistenceProbe, mockMembershipAgent)(
      actorSystem)
    underTest = underTestActor.underlyingActor
  }

  after {
    paxosSupervisorProbe.expectNoMsg((100 millis))
    actorSystem.shutdown()
  }

  describe("a SiriusPaxosActor") {
    it("should forward Put's to the persistence actor") {
      val put = Put("key", "body".getBytes)
      underTestActor ! put
      val OrderedEvent(_, _, actualPut) = persistenceProbe.receiveOne(5 seconds)
      assert(put == actualPut)
    }

    it("should increase its internal counter on Put's") {
      val origCount = underTest.seq
      underTestActor ! Put("key", "body".getBytes)
      val OrderedEvent(finalCount, _, _) = persistenceProbe.receiveOne(5 seconds)
      assertTrue(origCount < finalCount)
    }

    it("should forward Delete's to the persistence actor") {
      val del = Delete("key")
      underTestActor ! del
      val OrderedEvent(_, _, actualDel) = persistenceProbe.receiveOne(5 seconds)
      assert(del == actualDel)
    }

    it("should increase its internal counter on Delete's") {
      val origCount = underTest.seq
      underTestActor ! Delete("key")
      val OrderedEvent(finalCount, _, _) = persistenceProbe.receiveOne(5 seconds)
      assertTrue(origCount < finalCount)
    }

  }
}