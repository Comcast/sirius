package com.comcast.xfinity.sirius.api.impl.persistence

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog


import org.mockito.Mockito._
import org.mockito.Matchers._
import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.api.impl.{SiriusState, OrderedEvent, Put, Delete}
import akka.agent.Agent
import org.mockito.Matchers

class SiriusPersistenceActorTest extends NiceTest {

  var actorSystem: ActorSystem = _
  var underTestActor: TestActorRef[SiriusPersistenceActor] = _
  var testStateWorkerProbe: TestProbe = _
  var mockSiriusLog: SiriusLog = _
  var mockSiriusStateAgent: Agent[SiriusState] = _

  before {
    mockSiriusLog = mock[SiriusLog]
    mockSiriusStateAgent = mock[Agent[SiriusState]]
    actorSystem = ActorSystem("testsystem")
    testStateWorkerProbe = TestProbe()(actorSystem)
    underTestActor = TestActorRef(new SiriusPersistenceActor(testStateWorkerProbe.ref, mockSiriusLog, mockSiriusStateAgent))(actorSystem)

  }

  after {
    actorSystem.shutdown()
  }

  //actionType: String, key: String, sequence: Long, timestamp: Long, payload: Option[Array[Byte]]
  describe("a SiriusPersistenceActor") {

    //TODO: Should have tests for calls to mockSiriusLog

    it("should send an initialized message to StateActor on preStart()") {
      verify(mockSiriusStateAgent).send(any(classOf[SiriusState => SiriusState]
      ))
    }

    it("should forward Put's to the state actor") {
      when(mockSiriusLog.getNextSeq).thenReturn(0)

      val put = Put("key", "body".getBytes)
      val event = OrderedEvent(0L, 0L, put)

      underTestActor ! event
      testStateWorkerProbe.expectMsg(put)

      verify(mockSiriusLog, times(1)).writeEntry(event)
    }

    it("should forward Delete's to the state actor") {
      when(mockSiriusLog.getNextSeq).thenReturn(0)

      val delete = Delete("key")
      val event = OrderedEvent(0L, 0L, delete)

      underTestActor ! event
      testStateWorkerProbe.expectMsg(delete)

      verify(mockSiriusLog, times(1)).writeEntry(event)
    }

    it("should write two in-order events to the log") {
      when(mockSiriusLog.getNextSeq).thenReturn(0).thenReturn(1)

      val delete = Delete("key")
      val event1 = OrderedEvent(0L, 0L, delete)
      val event2 = OrderedEvent(1L, 0L, delete)

      underTestActor ! event1
      underTestActor ! event2

      verify(mockSiriusLog, times(2)).writeEntry(Matchers.any[OrderedEvent])
    }

    it("should buffer out-of-order events until they can be written") {
      val delete = Delete("key")
      val event1 = OrderedEvent(1L, 0L, delete)
      val event2 = OrderedEvent(2L, 0L, delete)
      val event3 = OrderedEvent(0L, 0L, delete)

      when(mockSiriusLog.getNextSeq).thenReturn(0)
      underTestActor ! event1
      underTestActor ! event2

      // no writes happening
      verify(mockSiriusLog, times(0)).writeEntry(Matchers.any[OrderedEvent])

      when(mockSiriusLog.getNextSeq).thenReturn(0).thenReturn(1).thenReturn(2)
      underTestActor ! event3
      // all three writes should go in
      verify(mockSiriusLog, times(3)).writeEntry(Matchers.any[OrderedEvent])

      assert(underTestActor.underlyingActor.orderedEventBuffer.isEmpty, "event buffer is not empty")
    }

    it("should ignore events with seq < nextSseq") {
      val delete = Delete("key")
      val event1 = OrderedEvent(0L, 0L, delete)
      val event2 = OrderedEvent(1L, 0L, delete)
      val event3 = OrderedEvent(0L, 0L, delete)

      when(mockSiriusLog.getNextSeq).thenReturn(0).thenReturn(1).thenReturn(2)
      underTestActor ! event1
      underTestActor ! event2

      // events 1 and 2 were written
      verify(mockSiriusLog, times(2)).writeEntry(Matchers.any[OrderedEvent])

      underTestActor ! event3

      // no new events written
      verify(mockSiriusLog, times(2)).writeEntry(Matchers.any[OrderedEvent])

      // event was not stored in buffer
      assert(underTestActor.underlyingActor.orderedEventBuffer.isEmpty, "event buffer is not empty")
    }
  }

}