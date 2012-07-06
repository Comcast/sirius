package com.comcast.xfinity.sirius.api.impl.persistence

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestActorRef}
import akka.util.duration._
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.api.impl.{Put, OrderedEvent}
import com.comcast.xfinity.sirius.writeaheadlog.{LogDataSerDe, LogData}

class LogReceivingActorTest extends NiceTest {

  var actorSystem : ActorSystem = _
  var receiver : TestActorRef[LogReceivingActor] = _
  var persistenceActorProbe: TestProbe = _
  var mockSender : TestProbe = _
  var mockSerializer: LogDataSerDe = _

  val testArray = Array[Byte](65, 124, 65)
  val logData = new LogData("PUT", "key1", 1L, 300392L, Some(testArray))

  before {
    actorSystem = ActorSystem("actorSystem")
    persistenceActorProbe = TestProbe()(actorSystem)
    mockSender = TestProbe()(actorSystem)
    mockSerializer = mock[LogDataSerDe]

    receiver = TestActorRef(new LogReceivingActor(persistenceActorProbe.ref, mockSerializer))(actorSystem)
  }

  describe("a LogReceivingActor") {
    it("handles a LogChunk message and sends a Received message") {

      val lines = Vector("a", "b", "c")

      when(mockSerializer.deserialize("a")).thenReturn(logData)
      when(mockSerializer.deserialize("b")).thenReturn(logData)
      when(mockSerializer.deserialize("c")).thenReturn(logData)

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

    it("should serialize a message received and send it to its persistenceActor") {
      val logString = "a"

      when(mockSerializer.deserialize(logString)).thenReturn(new LogData("PUT", "TEST", 1L, 1L, Some(testArray)))

      receiver ! LogChunk(1, List[String](logString))
      persistenceActorProbe.expectMsg(1 second, OrderedEvent(1L, 1L, Put("TEST", testArray)))
    }
  }
}
