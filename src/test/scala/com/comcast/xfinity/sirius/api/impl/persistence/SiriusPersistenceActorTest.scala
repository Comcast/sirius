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
      val put = Put("key", "body".getBytes)
      val event = OrderedEvent(0L, 0L, put)

      underTestActor ! event
      testStateWorkerProbe.expectMsg(put)

      verify(mockSiriusLog, times(1)).writeEntry(event)
    }

    it("should forward Delete's to the state actor") {
      val delete = Delete("key")
      val event = OrderedEvent(0L, 0L, delete)

      underTestActor ! event
      testStateWorkerProbe.expectMsg(delete)

      verify(mockSiriusLog, times(1)).writeEntry(event)
    }
  }

}