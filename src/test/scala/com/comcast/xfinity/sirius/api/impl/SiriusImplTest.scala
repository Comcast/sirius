package com.comcast.xfinity.sirius.api.impl

import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSpec

import com.comcast.xfinity.sirius.api.RequestHandler
import com.typesafe.config.ConfigFactory

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.Await
import akka.testkit.TestActor
import akka.testkit.TestProbe
import akka.util.duration._
import akka.util.Timeout

@RunWith(classOf[JUnitRunner])
class SiriusImplTest extends FunSpec with BeforeAndAfter {

  var mockRequestHandler: RequestHandler = _
  var stateActorProbe: TestProbe = _
  var underTest: SiriusImpl = _
  var spiedAkkaSystem: ActorSystem = _
  val timeout: Timeout = (5 seconds)

  before {
    spiedAkkaSystem = spy(ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """)))

    mockRequestHandler = mock(classOf[RequestHandler])
    stateActorProbe = TestProbe()(spiedAkkaSystem)
    stateActorProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] =
        msg match {
          case Delete(_) => sender ! "Delete it".getBytes(); Some(this)
          case Get(_) => sender ! "Got it".getBytes(); Some(this)
          case Put(_, _) => sender ! "Put it".getBytes(); Some(this)
        }
    })

    doReturn(stateActorProbe.ref).when(spiedAkkaSystem).actorOf(any(classOf[Props]), anyString())
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
      stateActorProbe.expectMsg(Put(key, body))
    }

    it("should forward a GET message to StateWorker when enqueueGet called ") {
      val key = "hello"
      assert("Got it".getBytes() === Await.result(underTest.enqueueGet(key), timeout.duration).asInstanceOf[Array[Byte]])
      stateActorProbe.expectMsg(Get(key))
    }

    it("should forward a DELETE message to StateWorker when enqueueDelete called ") {
      val key = "hello"
      assert("Delete it".getBytes() === Await.result(underTest.enqueueDelete(key), timeout.duration).asInstanceOf[Array[Byte]])
      stateActorProbe.expectMsg(Delete(key))
    }
  }
}