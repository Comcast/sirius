package com.comcast.xfinity.sirius.api.impl

import org.mockito.Matchers.any
import org.mockito.Mockito.doReturn
import org.mockito.Mockito.spy
import com.comcast.xfinity.sirius.api.RequestHandler
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.dispatch.Await
import akka.testkit.TestProbe
import akka.util.Timeout.durationToTimeout
import akka.util.duration.intToDurationInt
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import akka.testkit.TestActor
import org.mockito.Matchers
import com.comcast.xfinity.sirius.NiceTest

class SiriusImplTest extends NiceTest {

  var mockRequestHandler: RequestHandler = _
  var stateActorProbe: TestProbe = _
  var persistenceActorProbe: TestProbe = _
  var paxosActorProbe: TestProbe = _
  var underTest: SiriusImpl = _
  var spiedAkkaSystem: ActorSystem = _
  val timeout: Timeout = (5 seconds)

  before {
    spiedAkkaSystem = spy(ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """)))

    mockRequestHandler = mock[RequestHandler]
    stateActorProbe = TestProbe()(spiedAkkaSystem)
    stateActorProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
          case Get(_) => sender ! "Got it".getBytes(); Some(this)
      }
    })
    persistenceActorProbe = TestProbe()(spiedAkkaSystem)
    paxosActorProbe = TestProbe()(spiedAkkaSystem)
    paxosActorProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Delete(_) => sender ! "Delete it".getBytes(); Some(this)
        case Put(_, _) => sender ! "Put it".getBytes(); Some(this)
      }
    })

    doReturn(stateActorProbe.ref).when(spiedAkkaSystem).
            actorFor(Matchers.eq("user/sirius/state"))
    doReturn(persistenceActorProbe.ref).when(spiedAkkaSystem).
            actorFor(Matchers.eq("user/sirius/persistence"))
    doReturn(paxosActorProbe.ref).when(spiedAkkaSystem).
            actorFor(Matchers.eq("user/sirius/paxos"))
            
    underTest = new SiriusImpl(mockRequestHandler, spiedAkkaSystem)
  }

  after {
    spiedAkkaSystem.shutdown()
  }

  describe("a SiriusScalaImpl") {
    it("should send a Get message to the state actor when enqueueGet is called") {
      val key = "hello"
      assert("Got it".getBytes() === Await.result(underTest.enqueueGet(key), timeout.duration).asInstanceOf[Array[Byte]])
      stateActorProbe.expectMsg(Get(key))
    }
    
    it("should send a Put message to the persistence actor when enqueuePut is called and get some \"ACK\" back") {
      val key = "hello"
      val body = "there".getBytes()
      assert("Put it".getBytes() === Await.result(underTest.enqueuePut(key, body), timeout.duration).asInstanceOf[Array[Byte]])
      paxosActorProbe.expectMsg(Put(key, body))
    }

    it("should send a Delete message to the persistence worker when enqueueDelete is called and get some \"ACK\" back") {
      val key = "hello"
      assert("Delete it".getBytes() === Await.result(underTest.enqueueDelete(key), timeout.duration).asInstanceOf[Array[Byte]])
      paxosActorProbe.expectMsg(Delete(key))
    }
  }
}
