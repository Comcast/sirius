package com.comcast.xfinity.sirius.api.impl.bridge

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.{ReceiveTimeout, ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestProbe}
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.{LogSubrange, GetLogSubrange}

class GapFetcherTest extends NiceTest {

  implicit val actorSystem = ActorSystem("GapFetcherTest")

  def makeGapFetcher(firstGapSeq: Long = 1,
                      target: ActorRef = TestProbe().ref,
                      replyTo: ActorRef = TestProbe().ref,
                      chunkSize: Int = 5,
                      chunkReceiveTimeout: Int = 5) = {
    TestActorRef(
      new GapFetcher(firstGapSeq, target, replyTo, chunkSize, chunkReceiveTimeout)
    )
  }

  describe ("on initialization") {
    it ("should request the first chunk") {
      val targetProbe = TestProbe()
      val expectedRequest = GetLogSubrange(1, 5)

      makeGapFetcher(target = targetProbe.ref)

      targetProbe.expectMsg(expectedRequest)
    }
  }

  describe ("upon receiving a chunk") {
    it ("should send the events to replyTo") {
      val events = List[OrderedEvent](
        OrderedEvent(1, 1, Delete("1")), OrderedEvent(2, 2, Delete("2"))
      )
      val chunk = LogSubrange(events)
      val parentProbe = TestProbe()

      val underTest = makeGapFetcher(replyTo = parentProbe.ref)

      underTest ! chunk

      val receivedEvents = parentProbe.receiveN(2)

      assert(events === receivedEvents)
    }

    describe ("that fully fulfills the request") {
      it ("should request another chunk") {
        val events = List[OrderedEvent](
          OrderedEvent(1, 1, Delete("1")), OrderedEvent(2, 2, Delete("2")),
          OrderedEvent(3, 3, Delete("3")), OrderedEvent(4, 4, Delete("4")),
          OrderedEvent(5, 5, Delete("5"))
        )
        val chunk = LogSubrange(events)
        val parentProbe = TestProbe()
        val targetProbe = TestProbe()

        val underTest = makeGapFetcher(replyTo = parentProbe.ref, target = targetProbe.ref)
        targetProbe.expectMsg(GetLogSubrange(1, 5))

        underTest ! chunk

        targetProbe.expectMsg(GetLogSubrange(6, 10))
        targetProbe.expectNoMsg()
      }
    }
    describe ("that does not fully fulfill the request") {
      it ("should die quietly") {
        val events = List[OrderedEvent](
          OrderedEvent(1, 1, Delete("1")), OrderedEvent(2, 2, Delete("2")),
          OrderedEvent(3, 3, Delete("3")), OrderedEvent(4, 4, Delete("4"))
        )
        val chunk = LogSubrange(events)
        val parentProbe = TestProbe()
        val targetProbe = TestProbe()

        val underTest = makeGapFetcher(replyTo = parentProbe.ref, target = targetProbe.ref)
        targetProbe.expectMsg(GetLogSubrange(1, 5))

        underTest ! chunk

        targetProbe.expectNoMsg()
        assert(underTest.isTerminated)
      }
    }
  }

  describe ("upon receiving a ReceiveTimeout") {
    it ("should die quietly") {
      val underTest = makeGapFetcher()

      underTest ! ReceiveTimeout

      assert(underTest.isTerminated)
    }
  }
}
