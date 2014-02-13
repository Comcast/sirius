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
package com.comcast.xfinity.sirius.api.impl.bridge

import com.comcast.xfinity.sirius.NiceTest
import akka.actor.{Terminated, ReceiveTimeout, ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestProbe}
import scala.concurrent.duration._
import com.comcast.xfinity.sirius.api.impl.{Delete, OrderedEvent}
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.{LogSubrange, GetLogSubrange}
import org.mockito.Matchers
import com.comcast.xfinity.sirius.api.impl.bridge.PaxosStateBridge.RequestFromSeq

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
    it ("should send the chunk to replyTo") {
      val events = List[OrderedEvent](
        OrderedEvent(1, 1, Delete("1")), OrderedEvent(2, 2, Delete("2"))
      )
      val chunk = LogSubrange(1, 2, events)
      val parentProbe = TestProbe()

      val underTest = makeGapFetcher(replyTo = parentProbe.ref)

      underTest ! chunk

      val receivedChunk = parentProbe.receiveOne(100 milliseconds)

      assert(chunk === receivedChunk)
    }
  }

  describe ("upon receiving a RequestFromSeq message") {
    it ("should request another chunk from the target") {
      val targetProbe = TestProbe()

      val gapFetcher = makeGapFetcher(target = targetProbe.ref)

      targetProbe.expectMsg(GetLogSubrange(1, 5))

      gapFetcher ! RequestFromSeq(11)
      targetProbe.expectMsg(GetLogSubrange(11, 15))
    }
  }

  describe ("upon receiving a ReceiveTimeout") {
    it ("should die quietly") {
      val underTest = makeGapFetcher()
      val terminationProbe = TestProbe()

      terminationProbe.watch(underTest)

      underTest ! ReceiveTimeout

      terminationProbe.expectMsgClass(classOf[Terminated])
    }
  }
}
