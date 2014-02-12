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

import akka.actor.ActorSystem
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

  var actorSystem: ActorSystem = _
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

  describe("a SiriusPersistenceActor") {

    //TODO: Should have tests for calls to mockSiriusLog

    it("should report log size"){
      when(mockSiriusLog.size).thenReturn(500L);

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
      var weigtedAvg = 0L;
      for (x <- 0 to nums.size-1){
        nums(x)
        weigtedAvg = underTestActor.underlyingActor.weightedAvg(x+1,nums(x),weigtedAvg)
        areBe = areBe ++ Seq(weigtedAvg)
      }
      assert(shouldBe === areBe)
    }

    describe ("when responding to a GetLogSubrange request") {
      it ("should respond with the correct events") {
        val expectedEvents = List(
          OrderedEvent(1, 2, Delete("first jawn")),
          OrderedEvent(3, 4, Delete("second jawn"))
        )
        doReturn(expectedEvents.reverse).when(mockSiriusLog).
          foldLeftRange(anyLong, anyLong)(any[Symbol])(any[(Symbol, OrderedEvent) => Symbol]())
        doReturn(4L).when(mockSiriusLog).getNextSeq

        val senderProbe = TestProbe()(actorSystem)
        senderProbe.send(underTestActor, GetLogSubrange(1, 3))
        senderProbe.expectMsg(LogSubrange(1, 3, expectedEvents))

        verify(mockSiriusLog).foldLeftRange(meq(1L), meq(3L))(meq(List[OrderedEvent]()))(any[(List[OrderedEvent], OrderedEvent) => List[OrderedEvent]]())
      }

      it ("should respond with a correct startSeq and endSeq when it has all the requested events") {
        val expectedEvents = List(
          OrderedEvent(1, 2, Delete("first jawn")),
          OrderedEvent(3, 4, Delete("second jawn"))
        )
        doReturn(expectedEvents.reverse).when(mockSiriusLog).
          foldLeftRange(anyLong, anyLong)(any[Symbol])(any[(Symbol, OrderedEvent) => Symbol]())
        doReturn(4L).when(mockSiriusLog).getNextSeq

        val senderProbe = TestProbe()(actorSystem)
        senderProbe.send(underTestActor, GetLogSubrange(1, 3))
        senderProbe.expectMsg(LogSubrange(1, 3, expectedEvents))
      }

      it ("should respond with a correct startSeq and endSeq when it has only some of the requested events") {
        val expectedEvents = List(
          OrderedEvent(1, 2, Delete("first jawn")),
          OrderedEvent(3, 4, Delete("second jawn"))
        )
        doReturn(expectedEvents.reverse).when(mockSiriusLog).
          foldLeftRange(anyLong, anyLong)(any[Symbol])(any[(Symbol, OrderedEvent) => Symbol]())
        doReturn(4L).when(mockSiriusLog).getNextSeq

        val senderProbe = TestProbe()(actorSystem)
        senderProbe.send(underTestActor, GetLogSubrange(1, 4))
        senderProbe.expectMsg(LogSubrange(1, 3, expectedEvents))
      }

      it ("should respond with an empty but 'complete' subrange if there are no events available in this range") {
        val expectedEvents = List[OrderedEvent]()
        doReturn(expectedEvents.reverse).when(mockSiriusLog).
          foldLeftRange(anyLong, anyLong)(any[Symbol])(any[(Symbol, OrderedEvent) => Symbol]())
        doReturn(21L).when(mockSiriusLog).getNextSeq

        val senderProbe = TestProbe()(actorSystem)
        senderProbe.send(underTestActor, GetLogSubrange(11, 20))
        senderProbe.expectMsg(LogSubrange(11, 20, expectedEvents))
      }
    }
  }
}
