package com.comcast.xfinity.sirius.api.impl

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.testkit.TestActorRef
import org.mockito.Mockito._
import org.mockito.{Mockito, Matchers}
import com.comcast.xfinity.sirius.api.{RequestMethod, RequestHandler}
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
    it("should forward proper message to handler") {
      val method = RequestMethod.PUT
      val key = "key"
      val body = "value".getBytes()
      testActor !(RequestMethod.PUT, key, body)
      verify(mockRequestHandler).handle(Matchers.eq(method), Matchers.eq(key), Matchers.eq(body))
    }

    it("should not invoke handler when given invalid message id") {
      testActor ! "derp"
      verify(mockRequestHandler, Mockito.never()).handle(Matchers.any(classOf[RequestMethod]), Matchers.any(classOf[String]), Matchers.any(classOf[Array[Byte]]))
   }
  }
}