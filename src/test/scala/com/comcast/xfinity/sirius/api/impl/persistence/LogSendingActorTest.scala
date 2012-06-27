package com.comcast.xfinity.sirius.api.impl.persistence

import com.comcast.xfinity.sirius.NiceTest
import akka.testkit.{TestFSMRef, TestProbe, TestActorRef}
import org.mockito.Mockito._
import akka.util.duration._
import akka.actor.ActorSystem
import io.Source
import org.scalatest.BeforeAndAfterAll

class LogSendingActorTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem: ActorSystem = ActorSystem("testsystem")
  var actor: TestFSMRef[LSState, LSData, LogSendingActor] = _
  //var actor: TestActorRef[LogSendingActor] = _
  var receiverProbe: TestProbe = _
  var mockSource: Source = _
  var mockIterator: Source#LineIterator = _

  before {
    mockSource = mock[Source]
    mockIterator = mock[Source#LineIterator]

    receiverProbe = TestProbe()(actorSystem)
    actor = TestFSMRef(new LogSendingActor)
  }

  override def afterAll() {
    actorSystem.shutdown()
  }

  describe("a logSendingActor") {
    it("should be able to produce two chunks upon a Start call, given enough input") {

      when(mockSource.getLines()).thenReturn(Iterator("a", "b", "c", "d", "e"))

      actor ! Start(receiverProbe.ref, mockSource, 2)
      val actualFirstChunk = receiverProbe.receiveOne(5 seconds)
      testChunk(1, List("a", "b"), actualFirstChunk)
      actor ! Received(1)
      actor ! Processed(1)
      val actualSecondChunk = receiverProbe.receiveOne(5 seconds)
      testChunk(2, List("c", "d"), actualSecondChunk)

      assert(actor.stateName  == Sending)
      actor ! Received(2)
      assert(actor.stateName  == Waiting)
    }
    it("should return one chunk and then a done message for a single-entry source") {

      when(mockSource.getLines()).thenReturn(Iterator("a"))

      actor ! Start(receiverProbe.ref, mockSource, 2)
      val actualFirstChunk = receiverProbe.receiveOne(5 seconds)
      testChunk(1, List("a"), actualFirstChunk)
      actor ! Received(1)
      actor ! Processed(1)
      val secondMessage  = receiverProbe.receiveOne(5 seconds)

      assert(actor.stateName == Done)

      // check that we got a DoneMsg from actor
      secondMessage match {
        case DoneMsg => actor ! DoneAck
        case _ => fail("Did not receive DoneMsg")
      }

      assert(actor.isTerminated)
    }

  }
  def testChunk(seq: Int, chunk: Seq[String], realLogChunk: AnyRef) = {
    realLogChunk match {
      case LogChunk(realSeq: Int, realChunk: Seq[String]) =>
        assert(seq == realSeq)
        assert(chunk === realChunk)
        true
      case _ => fail("Did not get a LogChunk!")
    }

  }


}
