/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
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

  before {
    mockRequestHandler = mock[RequestHandler]

    testActor = TestActorRef(new SiriusStateActor(mockRequestHandler))
  }

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("A SiriusStateWorker") {

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
        val terminationProbe = TestProbe()
        terminationProbe.watch(testActor)
        val senderProbe = TestProbe()
        val key = "key"
        val body = "value".getBytes

        val theException = new RuntimeException("well this sucks")

        doThrow(theException).when(mockRequestHandler).
          handlePut(Matchers.eq(key), Matchers.same(body))

        senderProbe.send(testActor, Put(key, body))

        senderProbe.expectMsg(SiriusResult.error(theException))
        terminationProbe.expectNoMsg()
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
        val terminationProbe = TestProbe()
        val senderProbe = TestProbe()
        val key = "key"
        terminationProbe.watch(testActor)

        val theException = new RuntimeException("well this sucks")

        doThrow(theException).when(mockRequestHandler).
          handleGet(Matchers.eq(key))

        senderProbe.send(testActor, Get(key))

        senderProbe.expectMsg(SiriusResult.error(theException))
        terminationProbe.expectNoMsg()
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
        val terminationProbe = TestProbe()
        terminationProbe.watch(testActor)

        val theException = new RuntimeException("well this sucks")

        doThrow(theException).when(mockRequestHandler).
          handleDelete(Matchers.eq(key))

        senderProbe.send(testActor, Delete(key))

        senderProbe.expectMsg(SiriusResult.error(theException))
        terminationProbe.expectNoMsg()
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
