package com.comcast.xfinity.sirius.api.impl

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.testkit.TestActorRef
import org.mockito.Mockito._
import org.mockito.{Mockito, Matchers}
import com.comcast.xfinity.sirius.api.RequestHandler
import org.scalatest.{BeforeAndAfter, FunSpec}
import akka.actor.ActorSystem
import akka.util.Timeout
import akka.util.duration._
import com.typesafe.config.ConfigFactory

@RunWith(classOf[JUnitRunner])
class SiriusStateWorkerTest extends FunSpec with BeforeAndAfter {

  var mockRequestHandler: RequestHandler = _
  var testActor: TestActorRef[SiriusStateWorker] = _
  var underTest: SiriusStateWorker = _
  var spiedAkkaSystem: ActorSystem = _
  val timeout: Timeout = (5 seconds)

  before {
    spiedAkkaSystem = spy(ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """)))
    
    mockRequestHandler = mock(classOf[RequestHandler])
    testActor = TestActorRef(new SiriusStateWorker(mockRequestHandler))(spiedAkkaSystem)
    underTest = testActor.underlyingActor
  }

  after {
    spiedAkkaSystem.shutdown()
  }

  describe("a SiriusStateWorker") {
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