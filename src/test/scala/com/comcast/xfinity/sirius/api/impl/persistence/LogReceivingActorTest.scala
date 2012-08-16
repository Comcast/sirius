package com.comcast.xfinity.sirius.api.impl.persistence

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestActorRef}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}

class LogReceivingActorTest extends NiceTest {

  var actorSystem : ActorSystem = _
  var receiver : TestActorRef[LogReceivingActor] = _
  var targetProbe: TestProbe = _
  var senderProbe : TestProbe = _

  before {
    actorSystem = ActorSystem("actorSystem")
    targetProbe = TestProbe()(actorSystem)
    senderProbe = TestProbe()(actorSystem)

    receiver = TestActorRef(new LogReceivingActor(targetProbe.ref))(actorSystem)
  }

  describe("a LogReceivingActor") {
    it("handles a LogChunk message, sending it's data to the target," +
       " and sends a Received message") {

      val events = Vector(
        OrderedEvent(1, 1, Delete("a")),
        OrderedEvent(2, 1, Delete("b")),
        OrderedEvent(3, 1, Delete("c"))
      )

      val logChunk = LogChunk(1, events)

      senderProbe.send(receiver, logChunk)

      senderProbe.expectMsg(Received(1))
      events.foreach(targetProbe.expectMsg(_))
    }

    it("handles a done message and sends back a DoneAck") {
      val doneMessage = DoneMsg
      senderProbe.send(receiver, doneMessage)

      senderProbe.expectMsg(DoneAck)
    }
  }
}
