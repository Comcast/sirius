package com.comcast.xfinity.sirius.api.impl

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSpec, BeforeAndAfter}
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.comcast.xfinity.sirius.api.{RequestHandler, RequestMethod}
import akka.testkit.{TestActor, TestProbe}
import akka.actor.{ActorRef, Props}
import akka.dispatch.Await
import akka.actor.ActorSystem
import akka.util.duration._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

@RunWith(classOf[JUnitRunner])
class SiriusImplTest extends FunSpec with BeforeAndAfter {

  var mockRequestHandler: RequestHandler = _
  var stateWorkerProbe: TestProbe = _
  var underTest: SiriusImpl = _
  var spiedAkkaSystem: ActorSystem = _
  val timeout: Timeout = (5 seconds)

  before {
    spiedAkkaSystem = spy(ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """)))
    
    mockRequestHandler = mock(classOf[RequestHandler])
    stateWorkerProbe = TestProbe()(spiedAkkaSystem)
    stateWorkerProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] =
        msg match {
          case (RequestMethod.GET, _, _) => sender ! "Got it".getBytes(); Some(this)
          case (RequestMethod.PUT, _, _) => sender ! "Put it".getBytes(); Some(this)
        }
    })

    doReturn(stateWorkerProbe.ref).when(spiedAkkaSystem).actorOf(any(classOf[Props]), anyString())
    underTest = new SiriusImpl(mockRequestHandler, spiedAkkaSystem)
  }

  after {
    spiedAkkaSystem.shutdown()
  }


  describe("a SiriusScalaImpl") {
    it("should forward a PUT message to StateWorker when enqueuePut called and get an \"ACK\" back") {
      val key = "hello"
      val body = "there".getBytes()
      assert("Put it".getBytes() === Await.result(underTest.enqueuePut(key, body), timeout.duration).asInstanceOf[Array[Byte]])
      stateWorkerProbe.expectMsg((RequestMethod.PUT, key, body))
    }

    it("should forward a GET message to StateWorker when enqueueGet called ") {
      val key = "hello"
      assert("Got it".getBytes() === Await.result(underTest.enqueueGet(key), timeout.duration).asInstanceOf[Array[Byte]])
      stateWorkerProbe.expectMsg((RequestMethod.GET, key, null))
    }
  }
}