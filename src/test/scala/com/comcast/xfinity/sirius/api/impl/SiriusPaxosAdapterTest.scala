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

    it ("must return false if the incoming slotNum does not match the one it expects") {
      val probe = TestProbe()
      val paxosAdapter = new SiriusPaxosAdapter(membership, 10, probe.ref)

      paxosAdapter.performFun(Decision(11, Command(null, 1, Delete("z"))))
      probe.expectNoMsg()
    }

    it ("must send the event to the persistence layer, update the next expected sequence number, " +
        "and respond to the client with a RequestPerformed message") {
      val persistenceProbe = TestProbe()
      val paxosAdapter = new SiriusPaxosAdapter(membership, 2, persistenceProbe.ref)

      val clientProbe = TestProbe()
      paxosAdapter.performFun(Decision(2, Command(clientProbe.ref, 1, Delete("z")) ))
      assert(3 === paxosAdapter.currentSeq)
      clientProbe.expectMsg(RequestPerformed)
      persistenceProbe.expectMsg(OrderedEvent(2, 1, Delete("z")))
    }
  }

}