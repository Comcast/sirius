package com.comcast.xfinity.sirius.api.impl.persistence

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestActorRef}
import akka.util.duration._

class LogReceivingActorTest extends NiceTest {

  var actorSystem : ActorSystem = _
  var receiver : TestActorRef[LogReceivingActor] = _
  var mockSender : TestProbe = _

  before {

    actorSystem = ActorSystem("actorSystem")
    receiver = TestActorRef(new LogReceivingActor)(actorSystem)
    mockSender = TestProbe()(actorSystem)

  }

  describe("a LogReceivingActor") {
    it("Handles a LogChunk message and sends a Received message") {

      val lines = Vector("a", "b", "c")
      val logChunk = LogChunk(1, lines)

      mockSender.send(receiver, logChunk)

      val message = mockSender.receiveOne(5 seconds)
      assert (message === Received(1))
    }

    it("Handles a done message and sends back a DoneAck") {

      val doneMessage = DoneMsg
      mockSender.send(receiver, doneMessage)

      val message = mockSender.receiveOne(5 seconds)
      assert (message === DoneAck)
    }
  }


}
