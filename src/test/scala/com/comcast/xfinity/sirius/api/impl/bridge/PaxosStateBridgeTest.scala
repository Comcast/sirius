/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.comcast.xfinity.sirius.api.impl.bridge

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import akka.testkit.{TestActorRef, TestProbe}
import com.comcast.xfinity.sirius.api.{SiriusConfiguration, SiriusResult}
import com.comcast.xfinity.sirius.api.impl.bridge.PaxosStateBridge.ChildProvider
import scala.concurrent.duration._
import akka.actor._
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.DecisionHint
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Decision
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Command
import scala.util.Success
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.{EmptySubrange, PartialSubrange, CompleteSubrange}

class PaxosStateBridgeTest extends NiceTest with BeforeAndAfterAll with TimedTest {

  implicit val actorSystem = ActorSystem("PaxosStateBridgeTest")

  val config = new SiriusConfiguration
  val defaultMockMembershipHelper = mock[MembershipHelper]
  doReturn(Success(TestProbe().ref)).when(defaultMockMembershipHelper).getRandomMember
  def makeStateBridge(startingSeq: Long,
                      stateSupActor: ActorRef = TestProbe().ref,
                      siriusSupActor: ActorRef = TestProbe().ref,
                      membershipHelper: MembershipHelper = defaultMockMembershipHelper,
                      catchupSupervisor: ActorRef = TestProbe().ref,
                      chunkSize: Option[Int] = None) = {

    if (chunkSize != None) {
      config.setProp(SiriusConfiguration.LOG_REQUEST_CHUNK_SIZE, chunkSize.get)
    }

    val childProvider = new ChildProvider(config, membershipHelper) {
      override def createCatchupSupervisor()(implicit context: ActorContext) = catchupSupervisor
    }

    TestActorRef(new PaxosStateBridge(startingSeq, stateSupActor, siriusSupActor, childProvider, 2.minutes, config) {
      override def preStart() {}
      override def postStop() {}
    })
  }

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("when receiving a Decision message") {
    it("must not acknowledge an Decision below it's slotnum") {
      val stateProbe = TestProbe()
      val clientProbe = TestProbe()
      val stateBridge = makeStateBridge(10, stateSupActor = stateProbe.ref)

      stateBridge ! Decision(9, Command(clientProbe.ref, 1, Delete("z")))
      clientProbe.expectNoMsg(100 millis)
      stateProbe.expectNoMsg(100 millis)
    }

    it("must, in the presence of multiple Decisions for a slot, " +
      "only acknowledge and apply the first") {
      val stateProbe = TestProbe()
      val clientProbe = TestProbe()

      val stateBridge = makeStateBridge(10, stateSupActor = stateProbe.ref)

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
      val clientProbe = TestProbe()

      val stateBridge = makeStateBridge(10, stateSupActor = stateProbe.ref)

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
      val clientProbe = TestProbe()

      val stateBridge = makeStateBridge(10, stateSupActor = stateProbe.ref)

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
      val stateBridge = makeStateBridge(12, siriusSupActor = siriusSupProbe.ref)
      stateBridge !  Decision(12, Command(TestProbe().ref, 4, Delete("d")))
      siriusSupProbe.expectMsg(DecisionHint(12L))
    }

    it("should send a DecisionHint with multiple slots when an eventbuffer with gaps is filled"){
      val siriusSupProbe = TestProbe()
      val stateBridge = makeStateBridge(12, siriusSupActor = siriusSupProbe.ref)
      stateBridge !  Decision(13, Command(TestProbe().ref, 4, Delete("d")))
      stateBridge !  Decision(14, Command(TestProbe().ref, 4, Delete("d")))
      stateBridge !  Decision(12, Command(TestProbe().ref, 4, Delete("d")))

      siriusSupProbe.expectMsg(DecisionHint(11L))
      siriusSupProbe.expectMsg(DecisionHint(11L))
      siriusSupProbe.expectMsg(DecisionHint(14L))
    }
  }

  describe("upon receiving a InitiateCatchup message") {
    it("should ask the CatchupSupervisor to start catchup with the current seq") {
      val catchupProbe = TestProbe()
      val underTest = makeStateBridge(10, catchupSupervisor = catchupProbe.ref)

      underTest ! InitiateCatchup
      catchupProbe.expectMsg(InitiateCatchup(10))
    }
  }
  describe("upon receiving a CompleteSubrange") {
    it("should ask the CatchupSupervisor to continue catchup") {
      val catchupProbe = TestProbe()
      val underTest = makeStateBridge(10, catchupSupervisor = catchupProbe.ref)

      underTest ! InitiateCatchup
      catchupProbe.expectMsg(InitiateCatchup(10))

      underTest ! CompleteSubrange(10, 10, List[OrderedEvent]())
      catchupProbe.expectMsgClass(classOf[ContinueCatchup])
    }
  }
  describe("upon receiving a PartialSubrange") {
    it("should ask the CatchupSupervisor to stop catchup") {
      val catchupProbe = TestProbe()
      val underTest = makeStateBridge(10, catchupSupervisor = catchupProbe.ref)

      underTest ! InitiateCatchup
      catchupProbe.expectMsg(InitiateCatchup(10))

      underTest ! PartialSubrange(10, 10, List[OrderedEvent]())
      catchupProbe.expectMsg(StopCatchup)
    }
  }
  describe("upon receiving a EmptySubrange") {
    it("should ask the CatchupSupervisor to stop catchup") {
      val catchupProbe = TestProbe()
      val underTest = makeStateBridge(10, catchupSupervisor = catchupProbe.ref)

      underTest ! InitiateCatchup
      catchupProbe.expectMsg(InitiateCatchup(10))

      underTest ! EmptySubrange
      catchupProbe.expectMsg(StopCatchup)
    }
  }
  describe("upon receiving a PopulatedSubrange") {
    it("should send the useful events to the StateSupervisor") {
      val stateProbe = TestProbe()
      val event = mock[OrderedEvent]
      doReturn(10L).when(event).sequence
      val underTest = makeStateBridge(10, stateSupActor = stateProbe.ref)

      underTest ! InitiateCatchup
      underTest ! CompleteSubrange(10, 11, List(event))

      stateProbe.expectMsg(event)
    }
    it("should update nextSeq") {
      val underTest = makeStateBridge(10)

      underTest ! InitiateCatchup
      underTest ! CompleteSubrange(10, 11, List[OrderedEvent](event(10), event(11)))

      assert(12 === underTest.underlyingActor.nextSeq)
    }
    it("should not update nextSeq if the subrange was not useful") {
      val underTest = makeStateBridge(15)

      underTest ! InitiateCatchup
      underTest ! CompleteSubrange(10, 11, List[OrderedEvent](event(10), event(11)))

      assert(15 === underTest.underlyingActor.nextSeq)
    }
    it("should remove now-out-of-date events from the event buffer") {
      val underTest = makeStateBridge(10)
      underTest.underlyingActor.eventBuffer.put(11L, mock[OrderedEvent])

      underTest ! InitiateCatchup
      underTest ! CompleteSubrange(10, 11, List[OrderedEvent](event(10), event(11)))

      assert(!underTest.underlyingActor.eventBuffer.containsKey(11L))
    }
    it("should send a DecisionHint to the sirius supervisor") {
      val supervisorProbe = TestProbe()
      val underTest = makeStateBridge(10, siriusSupActor = supervisorProbe.ref)

      underTest ! InitiateCatchup
      underTest ! CompleteSubrange(10, 11, List(event(10), event(11)))

      supervisorProbe.expectMsg(DecisionHint(11))
    }
  }
  def event(sequence: Long) = OrderedEvent(sequence, 0L, Delete(String.valueOf(sequence)))
}
