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
package com.comcast.xfinity.sirius.api.impl.state

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog


import org.mockito.Mockito._
import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Put, Delete}
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor._
import org.mockito.Matchers.{any, eq => meq, anyLong}
import com.comcast.xfinity.sirius.api.SiriusConfiguration

class SiriusPersistenceActorTest extends NiceTest {

  implicit var actorSystem: ActorSystem = _
  var underTestActor: TestActorRef[SiriusPersistenceActor] = _
  var testStateWorkerProbe: TestProbe = _
  var mockSiriusLog: SiriusLog = _

  before {
    mockSiriusLog = mock[SiriusLog]
    actorSystem = ActorSystem("testsystem")
    testStateWorkerProbe = TestProbe()(actorSystem)
    underTestActor = TestActorRef(
      new SiriusPersistenceActor(testStateWorkerProbe.ref, mockSiriusLog, new SiriusConfiguration)
    )(actorSystem)

  }

  after {
    actorSystem.shutdown()
  }

  def makePersistenceActor(stateActor: ActorRef = TestProbe().ref,
                           siriusLog: SiriusLog = mock[SiriusLog]): TestActorRef[SiriusPersistenceActor] = {
    val config = new SiriusConfiguration
    TestActorRef(new SiriusPersistenceActor(stateActor, siriusLog, config))
  }

  def makeMockLog(events: List[OrderedEvent], nextSeq: Long = 1L): SiriusLog = {
    val mockLog = mock[SiriusLog]
    doReturn(events).when(mockLog).foldLeftRange(anyLong, anyLong)(any[Symbol])(anyFoldFun)
    doReturn(nextSeq).when(mockLog).getNextSeq

    mockLog
  }

  def anyFoldFun = any[(Symbol, OrderedEvent) => Symbol]()

  def verifyFoldLeftRanged(siriusLog: SiriusLog, start: Long, end: Long) {
    verify(siriusLog).foldLeftRange(meq(start), meq(end))(meq(List[OrderedEvent]()))(any[(List[OrderedEvent], OrderedEvent) => List[OrderedEvent]]())
  }

  describe("a SiriusPersistenceActor") {

    //TODO: Should have tests for calls to mockSiriusLog

    it("should report log size"){
      when(mockSiriusLog.size).thenReturn(500L)

      val senderProbe = TestProbe()(actorSystem)
      senderProbe.send(underTestActor, GetLogSize)
      senderProbe.expectMsg(500L)
    }

    it ("should forward Put's to the state actor") {
      when(mockSiriusLog.getNextSeq).thenReturn(0)

      val put = Put("key", "body".getBytes)
      val event = OrderedEvent(0L, 0L, put)

      underTestActor ! event
      testStateWorkerProbe.expectMsg(put)

      verify(mockSiriusLog, times(1)).writeEntry(event)
    }

    it ("should forward Delete's to the state actor") {
      when(mockSiriusLog.getNextSeq).thenReturn(0)

      val delete = Delete("key")
      val event = OrderedEvent(0L, 0L, delete)

      underTestActor ! event
      testStateWorkerProbe.expectMsg(delete)

      verify(mockSiriusLog, times(1)).writeEntry(event)
    }

    it ("should reply to GetNextLogSeq requests directly") {
      val expectedNextSeq = 101L

      doReturn(expectedNextSeq).when(mockSiriusLog).getNextSeq

      val senderProbe = TestProbe()(actorSystem)
      senderProbe.send(underTestActor, GetNextLogSeq)
      senderProbe.expectMsg(expectedNextSeq)
    }

    it ("should calc weighted averages right")
    {
      val nums = Seq(50L,100L,1L,1L)
      val shouldBe = Seq(50L,83L,42L,25L)
      var areBe = Seq[Long]()
      var weigtedAvg = 0L
      for (x <- 0 to nums.size-1){
        nums(x)
        weigtedAvg = underTestActor.underlyingActor.weightedAvg(x+1,nums(x),weigtedAvg)
        areBe = areBe ++ Seq(weigtedAvg)
      }
      assert(shouldBe === areBe)
    }

    describe("upon receiving a GetLogSubrange message") {
      describe("when we can fully reply") {
        it("should build the list of events and reply with it") {
          val senderProbe = TestProbe()

          val event1 = mock[OrderedEvent]
          val event2 = mock[OrderedEvent]
          val mockLog = makeMockLog(List(event2, event1), 10L)
          val underTest = makePersistenceActor(siriusLog = mockLog)

          senderProbe.send(underTest, GetLogSubrange(1, 2))

          verifyFoldLeftRanged(mockLog, 1, 2)
          senderProbe.expectMsg(CompleteSubrange(1, 2, List(event1, event2)))
        }
      }
      describe("when we can partially reply") {
        it("should build the list of events and reply with it") {
          val senderProbe = TestProbe()

          val event1 = mock[OrderedEvent]
          val event2 = mock[OrderedEvent]
          val mockLog = makeMockLog(List(event2, event1), 10L)
          val underTest = makePersistenceActor(siriusLog = mockLog)

          senderProbe.send(underTest, GetLogSubrange(8, 11))

          verifyFoldLeftRanged(mockLog, 8, 9)
          senderProbe.expectMsg(PartialSubrange(8, 9, List(event1, event2)))
        }
      }
      describe("when we can't send anything useful at all") {
        it("should send back an EmptySubrange message") {
          val senderProbe = TestProbe()

          val event1 = mock[OrderedEvent]
          val event2 = mock[OrderedEvent]
          val mockLog = makeMockLog(List(event2, event1), 5L)
          val underTest = makePersistenceActor(siriusLog = mockLog)

          senderProbe.send(underTest, GetLogSubrange(8, 11))

          senderProbe.expectMsg(EmptySubrange)
        }
      }
    }
  }
}
