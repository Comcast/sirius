package com.comcast.xfinity.sirius.api.impl.persistence

import com.comcast.xfinity.sirius.{Helper, NiceTest}
import akka.testkit.{TestFSMRef, TestProbe}
import org.mockito.Mockito._
import akka.util.duration._
import akka.actor.{LoggingFSM, ActorSystem}
import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.writeaheadlog.LogIteratorSource
import com.typesafe.config.ConfigFactory
import scalax.io.CloseableIterator
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent, Put}

class LogSendingActorTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem: ActorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.debug.lifecycle=on
    akka.actor.debug.fsm=on
    akka.loglevel=DEBUG
    """))

  var actor: TestFSMRef[LSState, LSData, LogSendingActor] = _
  //var actor: TestActorRef[LogSendingActor] = _
  var receiverProbe: TestProbe = _
  var mockSource: LogIteratorSource = _
  var mockIterator: Iterator[String] = _

  before {
    mockIterator = mock[Iterator[String]]

    receiverProbe = TestProbe()
    actor = TestFSMRef(new LogSendingActor with LoggingFSM[LSState, LSData])
  }

  override def afterAll() {
    actorSystem.shutdown()
  }

  describe("a logSendingActor") {
    it("should be able to produce two chunks upon a Start call, given enough input") {
      mockSource = Helper.createMockSource(
        OrderedEvent(1, 1, Delete("a")),
        OrderedEvent(2, 1, Delete("b")),
        OrderedEvent(3, 1, Delete("c")),
        OrderedEvent(4, 1, Delete("d")),
        OrderedEvent(5, 1, Delete("e"))
      )

      actor ! Start(receiverProbe.ref, mockSource, 2)
      val actualFirstChunk = receiverProbe.receiveOne(5 seconds)
      testChunk(1, List(OrderedEvent(1, 1, Delete("a")),
                        OrderedEvent(2, 1, Delete("b"))), actualFirstChunk)
      actor ! Received(1)
      actor ! Processed(1)
      val actualSecondChunk = receiverProbe.receiveOne(5 seconds)
      testChunk(2, List(OrderedEvent(3, 1, Delete("c")),
                        OrderedEvent(4, 1, Delete("d"))), actualSecondChunk)

      assert(actor.stateName  == Sending)
      actor ! Received(2)
      assert(actor.stateName  == Waiting)
    }

    it("should return one chunk and then a done message for a single-entry source") {

      mockSource = Helper.createMockSource(OrderedEvent(1, 1, Delete("a")))

      actor ! Start(receiverProbe.ref, mockSource, 2)
      val actualFirstChunk = receiverProbe.receiveOne(5 seconds)
      testChunk(1, List(OrderedEvent(1, 1, Delete("a"))), actualFirstChunk)
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
      mockSource = Helper.createMockSource(
        OrderedEvent(1, 1, Delete("a")),
        OrderedEvent(2, 1, Delete("b")),
        OrderedEvent(3, 1, Delete("c")),
        OrderedEvent(4, 1, Delete("d")),
        OrderedEvent(6, 1, Delete("e")),
        OrderedEvent(7, 1, Delete("f")),
        OrderedEvent(8, 1, Delete("g"))
      )

      val expected1 = Seq(
        OrderedEvent(1, 1, Delete("a")),
        OrderedEvent(2, 1, Delete("b")),
        OrderedEvent(3, 1, Delete("c"))
      )

      actor ! Start(receiverProbe.ref, mockSource, 3)
      receiverProbe.expectMsg(1 seconds, LogChunk(1, expected1))
    }

    it("should gather data appropriately when there's less than chunkSize available") {
      val data = Seq(
        OrderedEvent(1, 1, Delete("a")),
        OrderedEvent(2, 1, Delete("b")),
        OrderedEvent(3, 1, Delete("c")),
        OrderedEvent(4, 1, Delete("d")),
        OrderedEvent(6, 1, Delete("e")),
        OrderedEvent(7, 1, Delete("f")),
        OrderedEvent(8, 1, Delete("g"))
      )
      mockSource = Helper.createMockSource(data.iterator)

      actor ! Start(receiverProbe.ref, mockSource, 30)
      receiverProbe.expectMsg(1 seconds, LogChunk(1, data))
    }
  }

  def testChunk(seq: Int, chunk: Seq[OrderedEvent], realLogChunk: AnyRef) = {
    realLogChunk match {
      case LogChunk(realSeq, realChunk) =>
        assert(seq == realSeq)
        assert(chunk === realChunk)
        true
      case _ => fail("Did not get a LogChunk!")
    }
  }

}
