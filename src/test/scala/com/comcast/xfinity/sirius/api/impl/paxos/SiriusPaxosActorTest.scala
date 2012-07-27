package com.comcast.xfinity.sirius.api.impl.paxos

import akka.util.duration._

import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import org.mockito.Mockito._
import akka.agent.Agent
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import akka.actor.{ActorRef, ActorSystem}

class SiriusPaxosActorTest extends NiceTest with TimedTest {

  var actorSystem: ActorSystem = _
  var underTestActor: TestActorRef[SiriusPaxosActor] = _
  var underTest: SiriusPaxosActor = _
  var persistenceProbe: TestProbe = _
  var mockMembershipAgent: Agent[Set[ActorRef]] = _
  var paxosSupervisorProbe: TestProbe = _


  def createProbedSiriusPaxosActor(paxosSupervisorProbe: TestProbe,
                                   persistenceProbe: TestProbe,
                                   membershipAgent: Agent[Set[ActorRef]])
                                  (implicit as: ActorSystem): TestActorRef[SiriusPaxosActor] = {
    TestActorRef(new SiriusPaxosActor(persistenceProbe.ref, membershipAgent) {
      override def createPaxosSupervisor(memAgent: Agent[Set[ActorRef]], perfDec: Replica.PerformFun): ActorRef =
        paxosSupervisorProbe.ref
    })
  }

  before {
    actorSystem = ActorSystem("testsystem")
    persistenceProbe = TestProbe()(actorSystem)
    paxosSupervisorProbe = TestProbe()(actorSystem)
    mockMembershipAgent = mock[Agent[Set[ActorRef]]]
    when(mockMembershipAgent.get()).thenReturn(Set[ActorRef]())



    underTestActor = createProbedSiriusPaxosActor(paxosSupervisorProbe, persistenceProbe, mockMembershipAgent)(
      actorSystem)
    underTest = underTestActor.underlyingActor
  }

  after {
    paxosSupervisorProbe.expectNoMsg((100 millis))
    actorSystem.shutdown()
  }

  describe("a SiriusPaxosActor") {
    it("should blow up when sent a message") {
      underTestActor ! "yo"
      waitForTrue(Any => {
        underTestActor.isTerminated
      }, 5000, 500)
    }
  }

}