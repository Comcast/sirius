package com.comcast.xfinity.sirius.api.impl.bridge

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.{Decision, Command}
import akka.testkit.{TestActorRef, TestProbe}
import com.comcast.xfinity.sirius.api.SiriusResult
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, SiriusPaxosAdapter, Delete}

class PaxosStateBridgeTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("PaxosStateBridgeTest")

  describe("when receiving a Decision message") {
    it ("must not acknowledge an Decision below it's slotnum") {
      val stateProbe = TestProbe()
      val clientProbe = TestProbe()
      val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref))

      stateBridge ! Decision(9, Command(clientProbe.ref, 1, Delete("z")))
      clientProbe.expectNoMsg()
      stateProbe.expectNoMsg()
    }

    it ("must, in the presence of multiple Decisions for a slot, " +
        "only acknowledge and apply the first") {
      val stateProbe = TestProbe()
      val clientProbe = TestProbe()

      val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref))

      val theDecision = Decision(10, Command(clientProbe.ref, 1, Delete("z")))

      stateBridge ! theDecision
      clientProbe.expectMsg(SiriusResult.none())
      stateProbe.expectMsg(OrderedEvent(10, 1, Delete("z")))

      stateBridge ! theDecision
      clientProbe.expectNoMsg()
      stateProbe.expectNoMsg()
    }

    it ("must queue unready Decisions and apply them when their time comes") {
      val stateProbe = TestProbe()
      val clientProbe = TestProbe()

      val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref))

      stateBridge ! Decision(11, Command(clientProbe.ref, 1, Delete("a")))
      clientProbe.expectMsg(SiriusResult.none())
      stateProbe.expectNoMsg()

      stateBridge ! Decision(13, Command(clientProbe.ref, 2, Delete("b")))
      clientProbe.expectMsg(SiriusResult.none())
      stateProbe.expectNoMsg()

      stateBridge ! Decision(10, Command(clientProbe.ref, 3, Delete("c")))
      clientProbe.expectMsg(SiriusResult.none())
      stateProbe.expectMsg(OrderedEvent(10, 3, Delete("c")))
      stateProbe.expectMsg(OrderedEvent(11, 1, Delete("a")))

      stateBridge ! Decision(12, Command(clientProbe.ref, 4, Delete("d")))
      clientProbe.expectMsg(SiriusResult.none())
      stateProbe.expectMsg(OrderedEvent(12, 4, Delete("d")))
      stateProbe.expectMsg(OrderedEvent(13, 2, Delete("b")))
    }
  }
}