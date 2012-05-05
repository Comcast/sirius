package com.comcast.xfinity.sirius.api.impl

import org.junit.runner.RunWith
import org.junit.Before
import org.mockito.runners.MockitoJUnitRunner
import org.mockito.Matchers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.Spy
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestProbe
import org.junit.After
import org.junit.Test
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSpec, BeforeAndAfter}
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.comcast.xfinity.sirius.api.{AkkaTestConfig, RequestHandler, RequestMethod}

@RunWith(classOf[JUnitRunner])
class SiriusScalaImplTest extends FunSpec with BeforeAndAfter with AkkaTestConfig {

  var mockRequestHandler: RequestHandler = _
  var stateWorkerProbe: TestProbe = _
  var underTest: SiriusScalaImpl = _

  before {
    mockRequestHandler = mock(classOf[RequestHandler])
    stateWorkerProbe = TestProbe()
    doReturn(stateWorkerProbe.ref).when(spiedAkkaSystem).actorOf(any(classOf[Props]))
    underTest = new SiriusScalaImpl(mockRequestHandler, spiedAkkaSystem)
  }

  after {
    spiedAkkaSystem.shutdown()
  }


  describe("a SiriusScalaImpl") {
    it("should forward a PUT message to StateWorker when enqueuePut called") {
      val key = "hello"
      val body = "there".getBytes()
      underTest.enqueuePut(key, body)
      stateWorkerProbe.expectMsg((RequestMethod.PUT, key, body))
    }

    it("should forward a GET message to StateWorker when enqueueGet called ") {
      val key = "hello"
      underTest.enqueueGet(key)
      stateWorkerProbe.expectMsg((RequestMethod.GET, key, null))
    }
  }
}