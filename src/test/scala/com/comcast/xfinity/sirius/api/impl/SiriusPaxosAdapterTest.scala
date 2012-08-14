package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.NiceTest
import akka.testkit.TestProbe
import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import paxos.PaxosMessages.{Command, Decision}

class SiriusPaxosAdapterTest extends NiceTest with BeforeAndAfterAll {

  implicit val as = ActorSystem("SiriusPaxosAdapterTest")

  override def afterAll {
    as.shutdown()
  }

  describe("its perform function") {
    it("must send all Decisions to the provided ActorRef") {
      val testProbe = TestProbe()

      val performFun = SiriusPaxosAdapter.createPerformFun(testProbe.ref)

      val theDecision = Decision(1, Command(null, 2, Delete("3")))
      performFun(theDecision)
      testProbe.expectMsg(theDecision)
    }
  }

}