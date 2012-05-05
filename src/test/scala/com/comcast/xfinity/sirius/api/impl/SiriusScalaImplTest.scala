package com.comcast.xfinity.sirius.api.impl

import org.junit.runner.RunWith

import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSpec, BeforeAndAfter}
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.comcast.xfinity.sirius.api.{AkkaTestConfig, RequestHandler, RequestMethod}
import akka.testkit.{TestActor, TestProbe}
import akka.actor.{ActorRef, Props}

@RunWith(classOf[JUnitRunner])
class SiriusScalaImplTest extends FunSpec with BeforeAndAfter with AkkaTestConfig {

  var mockRequestHandler: RequestHandler = _
  var stateWorkerProbe: TestProbe = _
  var underTest: SiriusScalaImpl = _

  before {
    mockRequestHandler = mock(classOf[RequestHandler])
    stateWorkerProbe = TestProbe()
    stateWorkerProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] =
        msg match {
          case (RequestMethod.GET, _, _) => sender ! "Got it".getBytes(); Some(this)
          case (RequestMethod.PUT, _, _) => sender ! "Put it".getBytes(); Some(this)
        }
    })

    doReturn(stateWorkerProbe.ref).when(spiedAkkaSystem).actorOf(any(classOf[Props]), anyString())
    underTest = new SiriusScalaImpl(mockRequestHandler, spiedAkkaSystem)
  }

  after {
    spiedAkkaSystem.shutdown()
  }


  describe("a SiriusScalaImpl") {
    it("should forward a PUT message to StateWorker when enqueuePut called and get an \"ACK\" back") {
      val key = "hello"
      val body = "there".getBytes()
      assert("Put it".getBytes() === underTest.enqueuePut(key, body))
      stateWorkerProbe.expectMsg((RequestMethod.PUT, key, body))

    }

    it("should forward a GET message to StateWorker when enqueueGet called ") {
      val key = "hello"
      assert("Got it".getBytes() === underTest.enqueueGet(key))
      stateWorkerProbe.expectMsg((RequestMethod.GET, key, null))
    }
  }
}