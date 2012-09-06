package com.comcast.xfinity.sirius.api.impl.bridge

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.testkit.{TestActorRef, TestProbe}
import com.comcast.xfinity.sirius.api.SiriusResult
import com.comcast.xfinity.sirius.api.impl.bridge.PaxosStateBridge.RequestGaps
import com.comcast.xfinity.sirius.api.impl.persistence.{BoundedLogRange, RequestLogFromAnyRemote}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.{AkkaConfig, OrderedEvent, Delete}
import akka.actor.ActorSystem

class PaxosStateBridgeTest extends NiceTest with BeforeAndAfterAll with AkkaConfig {

  implicit val actorSystem = ActorSystem("PaxosStateBridgeTest")

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("when receiving a UnreadyDecisionsCountReq") {
    it("must return the count of buffered decisions") {
      val senderProbe = TestProbe()
      val stateBridge = TestActorRef(new PaxosStateBridge(10, TestProbe().ref, TestProbe().ref, TestProbe().ref))
      senderProbe.send(stateBridge, UnreadyDecisionsCountReq)
      senderProbe.expectMsg(UnreadyDecisionCount(0))

    }
  }

  describe("when receiving an OrderedEvent") {
    it("must queue unready events and apply them when their time comes") {
      val stateProbe = TestProbe()
      val logRequestProbe = TestProbe()

      val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref, logRequestProbe.ref, TestProbe().ref))


      stateBridge ! OrderedEvent(11, 1, Delete("a"))
      stateProbe.expectNoMsg(100 millis)
      assert(1 === stateBridge.underlyingActor.eventBuffer.size)

      stateBridge ! OrderedEvent(13, 2, Delete("b"))
      stateProbe.expectNoMsg(100 millis)
      assert(2 === stateBridge.underlyingActor.eventBuffer.size)

      stateBridge ! OrderedEvent(10, 3, Delete("c"))
      stateProbe.expectMsg(OrderedEvent(10, 3, Delete("c")))
      stateProbe.expectMsg(OrderedEvent(11, 1, Delete("a")))
      assert(1 === stateBridge.underlyingActor.eventBuffer.size)

      stateBridge ! OrderedEvent(12, 4, Delete("d"))
      stateProbe.expectMsg(OrderedEvent(12, 4, Delete("d")))
      stateProbe.expectMsg(OrderedEvent(13, 2, Delete("b")))
      assert(0 === stateBridge.underlyingActor.eventBuffer.size)
    }

    it ("should drop events with seq < nextSeq on the floor") {
      val stateProbe = TestProbe()
      val logRequestProbe = TestProbe()

      val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref, logRequestProbe.ref, TestProbe().ref))

      stateBridge ! OrderedEvent(9, 2, Delete("b"))
      stateProbe.expectNoMsg(100 millis)
      assert(0 === stateBridge.underlyingActor.eventBuffer.size)
      assert(10 === stateBridge.underlyingActor.nextSeq)
    }

    it ("should drop already seen events on the floor") {
      val stateProbe = TestProbe()
      val logRequestProbe = TestProbe()

      val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref, logRequestProbe.ref, TestProbe().ref))

      stateBridge ! OrderedEvent(11, 1, Delete("a"))
      stateProbe.expectNoMsg(100 millis)
      assert(1 === stateBridge.underlyingActor.eventBuffer.size)

      stateBridge ! OrderedEvent(11, 2, Delete("b"))
      stateProbe.expectNoMsg(100 millis)
      assert(1 === stateBridge.underlyingActor.eventBuffer.size)
    }
    it("should send a DecisionHint to the SiriusSupervisor with the latest decided slot"){
      val siriusSupProbe = TestProbe()
      val stateBridge = TestActorRef(new PaxosStateBridge(11, TestProbe().ref, TestProbe().ref, siriusSupProbe.ref))
      stateBridge ! OrderedEvent(11, 1, Delete("a"))
      siriusSupProbe.expectMsg(DecisionHint(11L))
    }

    it("should send a DecisionHint with a list of slots to the SiriusSupervisor when an eventbuffer with gaps gets filled"){
      val siriusSupProbe = TestProbe()
      val stateBridge = TestActorRef(new PaxosStateBridge(11, TestProbe().ref, TestProbe().ref, siriusSupProbe.ref))
      stateBridge ! OrderedEvent(12, 1, Delete("a"))
      stateBridge ! OrderedEvent(13, 1, Delete("a"))
      stateBridge ! OrderedEvent(11, 1, Delete("a"))
      siriusSupProbe.expectMsg(DecisionHint(13L))
    }


  }

  describe("when receiving a Decision message") {
    it("must not acknowledge an Decision below it's slotnum") {
      val stateProbe = TestProbe()
      val logRequestProbe = TestProbe()
      val clientProbe = TestProbe()
      val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref, logRequestProbe.ref, TestProbe().ref))

      stateBridge ! Decision(9, Command(clientProbe.ref, 1, Delete("z")))
      clientProbe.expectNoMsg(100 millis)
      stateProbe.expectNoMsg(100 millis)
    }

    it("must, in the presence of multiple Decisions for a slot, " +
      "only acknowledge and apply the first") {
      val stateProbe = TestProbe()
      val logRequestProbe = TestProbe()
      val clientProbe = TestProbe()

      val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref, logRequestProbe.ref, TestProbe().ref))

      val theDecision = Decision(10, Command(clientProbe.ref, 1, Delete("z")))

      stateBridge ! theDecision
      clientProbe.expectMsg(SiriusResult.none())
      stateProbe.expectMsg(OrderedEvent(10, 1, Delete("z")))

      stateBridge ! theDecision
      clientProbe.expectNoMsg(100 millis)
      stateProbe.expectNoMsg(100 millis)
    }

    it ("should drop already seen decisions on the floor") {
      val stateProbe = TestProbe()
      val logRequestProbe = TestProbe()
      val clientProbe = TestProbe()

      val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref, logRequestProbe.ref, TestProbe().ref))

      stateBridge ! Decision(11, Command(clientProbe.ref, 1, Delete("a")))
      stateProbe.expectNoMsg(100 millis)
      clientProbe.expectMsg(SiriusResult.none())
      assert(1 === stateBridge.underlyingActor.eventBuffer.size)

      stateBridge ! Decision(11, Command(clientProbe.ref, 1, Delete("a")))
      stateProbe.expectNoMsg(100 millis)
      clientProbe.expectNoMsg(100 millis)
      assert(1 === stateBridge.underlyingActor.eventBuffer.size)
    }

    it("must queue unready Decisions and apply them when their time comes") {
      val stateProbe = TestProbe()
      val logRequestProbe = TestProbe()
      val clientProbe = TestProbe()

      val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref, logRequestProbe.ref, TestProbe().ref))

      stateBridge ! Decision(11, Command(clientProbe.ref, 1, Delete("a")))
      clientProbe.expectMsg(SiriusResult.none())
      stateProbe.expectNoMsg(100 millis)
      assert(1 === stateBridge.underlyingActor.eventBuffer.size)

      stateBridge ! Decision(13, Command(clientProbe.ref, 2, Delete("b")))
      clientProbe.expectMsg(SiriusResult.none())
      stateProbe.expectNoMsg(100 millis)
      assert(2 === stateBridge.underlyingActor.eventBuffer.size)

      stateBridge ! Decision(10, Command(clientProbe.ref, 3, Delete("c")))
      clientProbe.expectMsg(SiriusResult.none())
      stateProbe.expectMsg(OrderedEvent(10, 3, Delete("c")))
      stateProbe.expectMsg(OrderedEvent(11, 1, Delete("a")))
      assert(1 === stateBridge.underlyingActor.eventBuffer.size)

      stateBridge ! Decision(12, Command(clientProbe.ref, 4, Delete("d")))
      clientProbe.expectMsg(SiriusResult.none())
      stateProbe.expectMsg(OrderedEvent(12, 4, Delete("d")))
      stateProbe.expectMsg(OrderedEvent(13, 2, Delete("b")))
      assert(0 === stateBridge.underlyingActor.eventBuffer.size)

    }
    it("should send a DecisionHint to the SiriusSupervisor with the last decided slot"){
      val siriusSupProbe = TestProbe()
      val stateBridge = TestActorRef(new PaxosStateBridge(12, TestProbe().ref, TestProbe().ref, siriusSupProbe.ref))
      stateBridge !  Decision(12, Command(TestProbe().ref, 4, Delete("d")))
      siriusSupProbe.expectMsg(DecisionHint(12L))
    }

    it("should send a DecisionHint with multiple slots when an eventbuffer with gaps is filled"){
      val siriusSupProbe = TestProbe()
      val stateBridge = TestActorRef(new PaxosStateBridge(12, TestProbe().ref, TestProbe().ref, siriusSupProbe.ref))
      stateBridge !  Decision(13, Command(TestProbe().ref, 4, Delete("d")))
      stateBridge !  Decision(14, Command(TestProbe().ref, 4, Delete("d")))
      stateBridge !  Decision(12, Command(TestProbe().ref, 4, Delete("d")))

      siriusSupProbe.expectMsg(DecisionHint(14L))
    }

  }

  it("must find no gaps for an empty event buffer") {
    val stateProbe = TestProbe()
    val logRequestProbe = TestProbe()

    val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref, logRequestProbe.ref, TestProbe().ref))

    stateBridge ! RequestGaps
    logRequestProbe.expectNoMsg(100 millis)
  }

  it("must be able to identify a single gap") {
    val stateProbe = TestProbe()
    val logRequestProbe = TestProbe()
    val clientProbe = TestProbe()

    val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref, logRequestProbe.ref, TestProbe().ref))

    stateBridge ! Decision(11, Command(clientProbe.ref, 1, Delete("a")))

    stateBridge ! RequestGaps
    logRequestProbe.expectMsg(RequestLogFromAnyRemote(BoundedLogRange(10, 10), stateBridge))
  }

  it("must be able to identify multiple gaps") {
    val stateProbe = TestProbe()
    val logRequestProbe = TestProbe()
    val clientProbe = TestProbe()

    val stateBridge = TestActorRef(new PaxosStateBridge(10, stateProbe.ref, logRequestProbe.ref, TestProbe().ref))

    stateBridge ! Decision(11, Command(clientProbe.ref, 1, Delete("a")))
    stateBridge ! Decision(15, Command(clientProbe.ref, 1, Delete("a")))
    stateBridge ! Decision(19, Command(clientProbe.ref, 1, Delete("a")))
    stateBridge ! Decision(20, Command(clientProbe.ref, 1, Delete("a")))
    stateBridge ! Decision(25, Command(clientProbe.ref, 1, Delete("a")))

    stateBridge ! RequestGaps

    logRequestProbe.expectMsg(RequestLogFromAnyRemote(BoundedLogRange(10, 10), stateBridge))
    logRequestProbe.expectMsg(RequestLogFromAnyRemote(BoundedLogRange(12, 14), stateBridge))
    logRequestProbe.expectMsg(RequestLogFromAnyRemote(BoundedLogRange(16, 18), stateBridge))
    logRequestProbe.expectMsg(RequestLogFromAnyRemote(BoundedLogRange(21, 24), stateBridge))
  }
}