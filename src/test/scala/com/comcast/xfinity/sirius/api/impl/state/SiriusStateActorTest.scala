package com.comcast.xfinity.sirius.api.impl.state

import org.mockito.Mockito.spy
import org.mockito.Mockito.verify
import org.mockito.Matchers
import org.mockito.Mockito
import com.comcast.xfinity.sirius.api.RequestHandler
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.util.Timeout.durationToTimeout
import akka.util.duration.intToDurationInt
import akka.util.Timeout
import com.comcast.xfinity.sirius.api.impl.{SiriusState, Delete, Get, Put}
import com.comcast.xfinity.sirius.NiceTest
import akka.agent.Agent

class SiriusStateActorTest extends NiceTest {

  var mockRequestHandler: RequestHandler = _
  var testActor: TestActorRef[SiriusStateActor] = _
  var underTest: SiriusStateActor = _
  var spiedAkkaSystem: ActorSystem = _
  var mockSiriusStateAgent: Agent[SiriusState] = _
  val timeout: Timeout = (5 seconds)

  before {
    spiedAkkaSystem = spy(ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """)))
    
    mockRequestHandler = mock[RequestHandler]
    mockSiriusStateAgent = mock[Agent[SiriusState]]

    testActor = TestActorRef(new SiriusStateActor(mockRequestHandler, mockSiriusStateAgent))(spiedAkkaSystem)
    underTest = testActor.underlyingActor

  }

  after {
    spiedAkkaSystem.shutdown()
  }

  describe("a SiriusStateWorker") {

    it("should send an initialized message to StateActor on preStart()") {
      verify(mockSiriusStateAgent).send(Matchers.any(classOf[SiriusState => SiriusState]
      ))
    }

    it("should forward proper PUT message to handler") {
      val key = "key"
      val body = "value".getBytes()
      testActor ! Put(key, body)
      verify(mockRequestHandler).handlePut(Matchers.eq(key), Matchers.eq(body))
    }

    it("should forward proper GET message to handler") {
      val key = "key"
      testActor ! Get(key)
      verify(mockRequestHandler).handleGet(Matchers.eq(key))
    }

    it("should forward proper DELETE message to handler") {
      val key = "key"
      testActor ! Delete(key)
      verify(mockRequestHandler).handleDelete(Matchers.eq(key))
    }

    it("should not invoke handler when given invalid message id") {
      testActor ! "derp"
      verify(mockRequestHandler, Mockito.never()).handleDelete(Matchers.any(classOf[String]))
      verify(mockRequestHandler, Mockito.never()).handleGet(Matchers.any(classOf[String]))
      verify(mockRequestHandler, Mockito.never()).handlePut(Matchers.any(classOf[String]), Matchers.any(classOf[Array[Byte]]))
   }
  }
}