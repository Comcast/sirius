package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.NiceTest
import akka.agent.Agent
import akka.testkit.TestProbe
import akka.actor.{ActorSystem, ActorRef}
import org.scalatest.BeforeAndAfterAll
import paxos.PaxosMessages.{Decision, Command, RequestPerformed}

class SiriusPaxosAdapterTest extends NiceTest with BeforeAndAfterAll {

  implicit val as = ActorSystem("SiriusPaxosAdapterTest")

  override def afterAll {
    as.shutdown()
  }

  describe("its perform function") {
    val membership = Agent(Set[ActorRef]())

    it ("must not acknowledge an Decision below it's slotnum") {
      val persistenceProbe = TestProbe()
      val clientProbe = TestProbe()
      val paxosAdapter = new SiriusPaxosAdapter(membership, 10, persistenceProbe.ref)

      paxosAdapter.performFun(Decision(9, Command(clientProbe.ref, 1, Delete("z"))))
      clientProbe.expectNoMsg()
      persistenceProbe.expectNoMsg()
    }

    it ("must only acknowledge a new Decision once") {
      val persistenceProbe = TestProbe()
      val clientProbe = TestProbe()

      val paxosAdapter = new SiriusPaxosAdapter(membership, 10, persistenceProbe.ref)

      val theDecision = Decision(10, Command(clientProbe.ref, 1, Delete("z")))

      paxosAdapter.performFun(theDecision)
      clientProbe.expectMsg(RequestPerformed)
      persistenceProbe.expectMsg(OrderedEvent(10, 1, Delete("z")))

      paxosAdapter.performFun(theDecision)
      clientProbe.expectNoMsg()
      persistenceProbe.expectNoMsg()
    }

    it ("must queue unready Decisions and apply them when their time comes") {
      val persistenceProbe = TestProbe()
      val clientProbe = TestProbe()

      val paxosAdapter = new SiriusPaxosAdapter(membership, 10, persistenceProbe.ref)

      paxosAdapter.performFun(Decision(11, Command(clientProbe.ref, 1, Delete("a"))))
      clientProbe.expectMsg(RequestPerformed)
      persistenceProbe.expectNoMsg()

      paxosAdapter.performFun(Decision(13, Command(clientProbe.ref, 2, Delete("b"))))
      clientProbe.expectMsg(RequestPerformed)
      persistenceProbe.expectNoMsg()

      paxosAdapter.performFun(Decision(10, Command(clientProbe.ref, 3, Delete("c"))))
      clientProbe.expectMsg(RequestPerformed)
      persistenceProbe.expectMsg(OrderedEvent(10, 3, Delete("c")))
      persistenceProbe.expectMsg(OrderedEvent(11, 1, Delete("a")))

      paxosAdapter.performFun(Decision(12, Command(clientProbe.ref, 4, Delete("d"))))
      clientProbe.expectMsg(RequestPerformed)
      persistenceProbe.expectMsg(OrderedEvent(12, 4, Delete("d")))
      persistenceProbe.expectMsg(OrderedEvent(13, 2, Delete("b")))
    }

  }

}