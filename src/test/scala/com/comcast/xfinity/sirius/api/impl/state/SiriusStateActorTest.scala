package com.comcast.xfinity.sirius.api.impl.state

import org.mockito.Matchers
import org.mockito.Mockito._
import akka.actor.ActorSystem
import com.comcast.xfinity.sirius.api.impl.{SiriusState, Delete, Get, Put}
import com.comcast.xfinity.sirius.NiceTest
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.{SiriusResult, RequestHandler}
import akka.testkit.{TestProbe, TestActorRef}
import org.scalatest.BeforeAndAfterAll

class SiriusStateActorTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("SiriusStateActorTest")

  var mockRequestHandler: RequestHandler = _
  var testActor: TestActorRef[SiriusStateActor] = _
  var mockSiriusStateAgent: Agent[SiriusState] = _

  before {
    mockRequestHandler = mock[RequestHandler]
    mockSiriusStateAgent = mock[Agent[SiriusState]]

    testActor = TestActorRef(new SiriusStateActor(mockRequestHandler, mockSiriusStateAgent))
  }

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("A SiriusStateWorker") {

    describe("on startup") {
      it("should send an initialized message to StateActor on preStart()") {
        verify(mockSiriusStateAgent).send(Matchers.any[SiriusState => SiriusState])
      }
    }

    describe("in response to a Put message") {
      it ("must forward the message to handler and reply with the result " +
          "when it succeeds") {
        val senderProbe = TestProbe()
        val key = "key"
        val body = "value".getBytes

        // the following is a little delicate, Array[Byte] are Java jawns, so
        //  we must use the same array for comparing
        doReturn(SiriusResult.some("It's alive!")).when(mockRequestHandler).
          handlePut(Matchers.eq(key), Matchers.same(body))

        senderProbe.send(testActor, Put(key, body))

        senderProbe.expectMsg(SiriusResult.some("It's alive!"))
      }

      it ("must forward the message to the handler and reply with a result " +
          "wrapping the exception when such occurs") {
        val senderProbe = TestProbe()
        val key = "key"
        val body = "value".getBytes

        val theException = new RuntimeException("well this sucks")

        doThrow(theException).when(mockRequestHandler).
          handlePut(Matchers.eq(key), Matchers.same(body))

        senderProbe.send(testActor, Put(key, body))

        senderProbe.expectMsg(SiriusResult.error(theException))
      }
    }

    describe("in response to a Get message") {
      it("must forward the message to handler and reply with the result when " +
          "it succeeds") {
        val senderProbe = TestProbe()
        val key = "key"

        doReturn(SiriusResult.some("It really works!")).when(mockRequestHandler).
          handleGet(Matchers.eq(key))

        senderProbe.send(testActor, Get(key))

        senderProbe.expectMsg(SiriusResult.some("It really works!"))
      }

      it ("must forward the message to the handler and reply with a result " +
          "wrapping the exception when such occurs") {
        val senderProbe = TestProbe()
        val key = "key"

        val theException = new RuntimeException("well this sucks")

        doThrow(theException).when(mockRequestHandler).
          handleGet(Matchers.eq(key))

        senderProbe.send(testActor, Get(key))

        senderProbe.expectMsg(SiriusResult.error(theException))
      }
    }

    describe("in response to a Delete message") {
      it ("should forward the message to handler and reply with the result when " +
          "it succeeds") {
        val senderProbe = TestProbe()
        val key = "key"

        doReturn(SiriusResult.some("I ate too much")).when(mockRequestHandler).
          handleDelete(Matchers.eq(key))

        senderProbe.send(testActor, Delete(key))

        senderProbe.expectMsg(SiriusResult.some("I ate too much"))
      }

      it ("must forward the message to the handler and reply with a result " +
          "wrapping the exception when such occurs") {
        val senderProbe = TestProbe()
        val key = "key"

        val theException = new RuntimeException("well this sucks")

        doThrow(theException).when(mockRequestHandler).
          handleDelete(Matchers.eq(key))

        senderProbe.send(testActor, Delete(key))

        senderProbe.expectMsg(SiriusResult.error(theException))
      }
    }

    describe("in response to all other messages") {
      it ("should do nothing") {
        intercept[MatchError] {
          testActor.underlyingActor.receive("derp")
        }
      }
    }
  }
}