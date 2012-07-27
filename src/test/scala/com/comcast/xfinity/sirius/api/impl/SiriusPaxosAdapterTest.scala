package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.NiceTest
import akka.agent.Agent
import akka.testkit.TestProbe
import akka.actor.{ActorSystem, ActorRef}
import org.scalatest.BeforeAndAfterAll
import akka.util.duration._

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

      assert(false === paxosAdapter.performFun(11, Delete("z")))
      probe.expectNoMsg()
    }

    it ("must send the event to the persistence layer, update the next expected sequence number, " +
        "and true if the incoming slotNum is the one it expects") {
      val probe = TestProbe()
      val paxosAdapter = new SiriusPaxosAdapter(membership, 2, probe.ref)

      assert(paxosAdapter.performFun(2, Delete("z")))
      assert(3 === paxosAdapter.currentSeq)
      probe.receiveOne(1 second) match {
        case OrderedEvent(seq, ts, req) =>
          assert(2 === seq)
          assert(System.currentTimeMillis() - ts < 3000)
          assert(Delete("z") === req)
      }
    }
  }

}