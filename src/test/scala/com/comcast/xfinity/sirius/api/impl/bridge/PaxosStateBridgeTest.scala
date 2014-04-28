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
import com.comcast.xfinity.sirius.api.impl.bridge.PaxosStateBridge.{ChildProvider, RequestGaps}
import scala.concurrent.duration._
import akka.actor._
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import org.mockito.Mockito._
import collection.SortedMap
import collection.JavaConversions._
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.LogSubrange
import com.comcast.xfinity.sirius.api.impl.OrderedEvent
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.DecisionHint
import scala.Some
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Decision
import com.comcast.xfinity.sirius.api.impl.bridge.PaxosStateBridge.RequestFromSeq
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Command
import scala.util.Success

class PaxosStateBridgeTest extends NiceTest with BeforeAndAfterAll with TimedTest {

  implicit val actorSystem = ActorSystem("PaxosStateBridgeTest")

  val config = new SiriusConfiguration
  val defaultMockMembershipHelper = mock[MembershipHelper]
  doReturn(Success(TestProbe().ref)).when(defaultMockMembershipHelper).getRandomMember
  def makeStateBridge(startingSeq: Long,
                      stateSupActor: ActorRef = TestProbe().ref,
                      siriusSupActor: ActorRef = TestProbe().ref,
                      membershipHelper: MembershipHelper = defaultMockMembershipHelper,
                      gapFetcherActor: ActorRef = TestProbe().ref,
                      chunkSize: Option[Int] = None) = {

    if (chunkSize != None) {
      config.setProp(SiriusConfiguration.LOG_REQUEST_CHUNK_SIZE, chunkSize.get)
    }

    val childProvider = new ChildProvider(config) {
      override def createGapFetcher(seq: Long, target: ActorRef, requester: ActorRef)(implicit context: ActorContext) = gapFetcherActor
    }

    TestActorRef(new PaxosStateBridge(startingSeq, stateSupActor, siriusSupActor, membershipHelper, childProvider, config) {
      override def preStart() {}
      override def postStop() {
        requestGapsCancellable.cancel()
      }
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

      siriusSupProbe.expectMsg(DecisionHint(14L))
    }
  }

  describe("when receiving a Terminated message") {
    it("should set gapFetcher to None if the terminated actor matches the current fetcher") {
      val gapFetcherActor = TestProbe()
      val underTest = makeStateBridge(10, gapFetcherActor = gapFetcherActor.ref)
      underTest ! RequestGaps
      assert(waitForTrue(Some(gapFetcherActor.ref) == underTest.underlyingActor.gapFetcher, 1000, 25))

      actorSystem.stop(gapFetcherActor.ref)

      assert(waitForTrue(None == underTest.underlyingActor.gapFetcher, 1000, 25))
    }
  }

  describe("upon receiving a RequestGaps message") {
    it ("must start a new GapFetcher if gapFetcher == None") {
      val gapFetcherActor = TestProbe()
      val stateBridge = makeStateBridge(10, gapFetcherActor = gapFetcherActor.ref)

      stateBridge.underlyingActor.gapFetcher = None
      stateBridge ! RequestGaps
      assert(Some(gapFetcherActor.ref) === stateBridge.underlyingActor.gapFetcher)
    }

    it ("must not start a new GapFetcher if one already exists") {
      val gapFetcherActor = TestProbe()
      val gapFetcherActorAlreadyRunning = TestProbe()
      val stateBridge = makeStateBridge(10, gapFetcherActor = gapFetcherActor.ref)

      stateBridge.underlyingActor.gapFetcher = Some(gapFetcherActorAlreadyRunning.ref)

      stateBridge ! RequestGaps
      assert(Some(gapFetcherActorAlreadyRunning.ref) === stateBridge.underlyingActor.gapFetcher)
    }
  }

  describe ("upon receiving a LogSubrange message") {
    describe ("that does not contain useful events") {
      it ("should ignore it") {
        val gapFetcher = TestProbe()
        val stateSup = TestProbe()
        val underTest = makeStateBridge(10, gapFetcherActor = gapFetcher.ref, stateSupActor = stateSup.ref)
        underTest ! LogSubrange(8, 9, List(
          OrderedEvent(8, 1, Delete("1")), OrderedEvent(9, 1, Delete("2"))
        ))

        gapFetcher.expectNoMsg()
        stateSup.expectNoMsg()
      }
    }

    describe ("that contains useful events (event.seq >= nextSeq)") {
      it ("should process the events") {
        val stateSup = TestProbe()
        val underTest = makeStateBridge(10, stateSupActor = stateSup.ref)
        underTest ! LogSubrange(9, 11, List(
          OrderedEvent(9, 1, Delete("1")), OrderedEvent(10, 1, Delete("2")), OrderedEvent(11, 1, Delete("3"))
        ))

        val expectedMessages = List(OrderedEvent(10, 1, Delete("2")), OrderedEvent(11, 1, Delete("3")))
        val stateMessages = stateSup.receiveN(2)
        assert(expectedMessages === stateMessages)
      }

      it ("should update nextSeq to rangeEnd + 1") {
        val underTest = makeStateBridge(10)
        underTest ! LogSubrange(9, 11, List(
          OrderedEvent(9, 1, Delete("1")), OrderedEvent(10, 1, Delete("2")), OrderedEvent(11, 1, Delete("3"))
        ))
        assert(12 === underTest.underlyingActor.nextSeq)
      }

      it ("should drop eventBuffer items < the updated nextSeq") {
        val underTest = makeStateBridge(10)
        underTest.underlyingActor.eventBuffer.putAll(SortedMap[Long, OrderedEvent](
          10L -> OrderedEvent(10, 1, Delete("2")),
          13L -> OrderedEvent(13, 1, Delete("5"))
        ))
        underTest ! LogSubrange(9, 11, List(
          OrderedEvent(9, 1, Delete("1")), OrderedEvent(10, 1, Delete("2")), OrderedEvent(11, 1, Delete("3"))
        ))

        assert(1 === underTest.underlyingActor.eventBuffer.size)
        assert(underTest.underlyingActor.eventBuffer.keySet.contains(13L))
      }

      it ("send a decision hint to siriusSup") {
        val siriusSup = TestProbe()
        val underTest = makeStateBridge(10, siriusSupActor = siriusSup.ref)
        underTest ! LogSubrange(9, 11, List(
          OrderedEvent(9, 1, Delete("1")), OrderedEvent(10, 1, Delete("2")), OrderedEvent(11, 1, Delete("3"))
        ))

        siriusSup.expectMsg(DecisionHint(11))
      }

      describe ("if the gapFetcher is alive and well") {
        it ("should ask for another chunk if it got a full chunk") {
          val gapFetcher = TestProbe()
          val underTest = makeStateBridge(10, gapFetcherActor = gapFetcher.ref, chunkSize = Some(2))
          underTest.underlyingActor.gapFetcher = Some(gapFetcher.ref)

          underTest ! LogSubrange(10, 11, List(
            OrderedEvent(10, 1, Delete("2")), OrderedEvent(11, 1, Delete("3"))
          ))

          gapFetcher.expectMsg(RequestFromSeq(12L))
        }

        it ("should not ask for anything further if the chunk was not complete") {
          val gapFetcher = TestProbe()
          val underTest = makeStateBridge(10, gapFetcherActor = gapFetcher.ref, chunkSize = Some(3))
          underTest.underlyingActor.gapFetcher = Some(gapFetcher.ref)

          underTest ! LogSubrange(10, 11, List(
            OrderedEvent(10, 1, Delete("2")), OrderedEvent(11, 1, Delete("3"))
          ))

          gapFetcher.expectNoMsg()
        }

        it ("should correctly accept a subrange as 'complete', even if it has no events") {
          val gapFetcher = TestProbe()
          val underTest = makeStateBridge(1, gapFetcherActor = gapFetcher.ref, chunkSize = Some(10))
          underTest.underlyingActor.gapFetcher = Some(gapFetcher.ref)

          underTest ! LogSubrange(1, 10, List())

          gapFetcher.expectMsg(RequestFromSeq(11L))
        }
      }

      describe ("if the gapFetcher is nonexistent") {
        it ("should do nothing further") {
          val gapFetcher = TestProbe()
          val underTest = makeStateBridge(10, gapFetcherActor = gapFetcher.ref)
          underTest.underlyingActor.gapFetcher = None

          underTest ! LogSubrange(9, 11, List(
            OrderedEvent(9, 1, Delete("1")), OrderedEvent(10, 1, Delete("2")), OrderedEvent(11, 1, Delete("3"))
          ))

          gapFetcher.expectNoMsg()
        }
      }

      describe ("if the gapFetcher is dead") {
        it ("should do nothing further") {
          val gapFetcher = TestProbe()
          val underTest = makeStateBridge(10, gapFetcherActor = gapFetcher.ref)
          underTest.underlyingActor.gapFetcher = Some(gapFetcher.ref)
          gapFetcher.ref ! PoisonPill

          underTest ! LogSubrange(9, 11, List(
            OrderedEvent(9, 1, Delete("1")), OrderedEvent(10, 1, Delete("2")), OrderedEvent(11, 1, Delete("3"))
          ))

          gapFetcher.expectNoMsg()
        }
      }
    }

  }
}
