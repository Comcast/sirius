package com.comcast.xfinity.sirius.api.impl.persistence

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestActorRef}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}

class LogReceivingActorTest extends NiceTest {

  var actorSystem : ActorSystem = _
  var receiver : TestActorRef[LogReceivingActor] = _
  var persistenceActorProbe: TestProbe = _
  var mockSender : TestProbe = _

  before {
    actorSystem = ActorSystem("actorSystem")
    persistenceActorProbe = TestProbe()(actorSystem)
    mockSender = TestProbe()(actorSystem)

    receiver = TestActorRef(new LogReceivingActor(persistenceActorProbe.ref))(actorSystem)
  }

  describe("a LogReceivingActor") {
    it("handles a LogChunk message and sends a Received message") {

      val lines = Vector(
        OrderedEvent(1, 1, Delete("a")),
        OrderedEvent(2, 1, Delete("b")),
        OrderedEvent(3, 1, Delete("c"))
      )

      val logChunk = LogChunk(1, lines)

      mockSender.send(receiver, logChunk)

      val message = mockSender.receiveOne(5 seconds)
      assert (message === Received(1))
    }

    it("handles a done message and sends back a DoneAck") {
      val doneMessage = DoneMsg
      mockSender.send(receiver, doneMessage)

      val message = mockSender.receiveOne(5 seconds)
      assert (message === DoneAck)
    }
  }
}
