package com.comcast.xfinity.sirius.api.impl.persistence

import com.comcast.xfinity.sirius.{TestHelper, NiceTest}
import akka.testkit.{TestFSMRef, TestProbe}
import org.mockito.Mockito._
import akka.util.duration._
import akka.actor.{LoggingFSM, ActorSystem}
import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.writeaheadlog.LogLinesSource
import com.typesafe.config.ConfigFactory
import scalax.io.CloseableIterator

class LogSendingActorTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem: ActorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.debug.lifecycle=on
    akka.actor.debug.fsm=on
    akka.loglevel=DEBUG
    """))
  var actor: TestFSMRef[LSState, LSData, LogSendingActor] = _
  //var actor: TestActorRef[LogSendingActor] = _
  var receiverProbe: TestProbe = _
  var mockSource: LogLinesSource = _
  var mockIterator: Iterator[String] = _

  before {
    mockIterator = mock[Iterator[String]]

    receiverProbe = TestProbe()(actorSystem)
    actor = TestFSMRef(new LogSendingActor with LoggingFSM[LSState, LSData])
  }

  override def afterAll() {
    actorSystem.shutdown()
  }

  describe("a logSendingActor") {
    it("should be able to produce two chunks upon a Start call, given enough input") {

      mockSource = TestHelper.createMockSource("a", "b", "c", "d", "e")

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

      mockSource = TestHelper.createMockSource("a")

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
    it("should fail and die when sent an unexpected message") {
      actor ! DoneAck
      assert(actor.isTerminated)
    }
    it("should gather data appropriately when there's plenty to grab") {
      mockSource = TestHelper.createMockSource("a", "b", "c", "d", "e", "f", "g")

      val expected1 = Seq("a", "b", "c")

      actor ! Start(receiverProbe.ref, mockSource, 3)
      receiverProbe.expectMsg(1 seconds, LogChunk(1, expected1))
    }
    it("should gather data appropriately when there's less than chunkSize available") {
      val data = Seq("a", "b", "c", "d", "e", "f", "g")
      mockSource = TestHelper.createMockSource(data.iterator)

      actor ! Start(receiverProbe.ref, mockSource, 30)
      receiverProbe.expectMsg(1 seconds, LogChunk(1, data))
    }
  }
  def testChunk(seq: Int, chunk: Seq[String], realLogChunk: AnyRef) = {
    realLogChunk match {
      case LogChunk(realSeq, realChunk) =>
        assert(seq == realSeq)
        assert(chunk === realChunk)
        true
      case _ => fail("Did not get a LogChunk!")
    }

  }


}
