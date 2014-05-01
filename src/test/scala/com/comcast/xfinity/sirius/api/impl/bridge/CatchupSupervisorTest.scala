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
import akka.testkit.{TestProbe, TestActorRef}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Helpers.base64
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import org.mockito.Mockito._
import scala.util.{Try, Success}
import java.util.concurrent.atomic.AtomicLong
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor._
import com.comcast.xfinity.sirius.api.SiriusConfiguration

class CatchupSupervisorTest extends NiceTest {
  implicit val actorSystem = ActorSystem("CatchupSupervisorTest")

  val atomicLong = new AtomicLong
  def makeMockCatchupSupervisor(remoteActorTry: Try[ActorRef] = Success(TestProbe().ref),
                                maxWindowSize: Int = 1000,
                                startingSSThresh: Int = 500,
                                parent: ActorRef = TestProbe().ref): TestActorRef[CatchupSupervisor] = {
    val membershipHelper = mock[MembershipHelper]
    doReturn(remoteActorTry).when(membershipHelper).getRandomMember

    val timeoutCoeff = 1.0
    val timeoutConst = .1

    // in order to specify the parent, we also have to specify a name for this actor. using akka's approach.
    val name = "$" + base64(atomicLong.getAndIncrement)
    val props = Props(new CatchupSupervisor(membershipHelper, timeoutCoeff, timeoutConst,
                                            maxWindowSize, startingSSThresh, new SiriusConfiguration()))
    TestActorRef(props, parent, name)
  }

  describe("CatchupSupervisor") {
    it("should request the next subrange if it gets an InitiateCatchup message") {
      val remote = TestProbe()
      val underTest = makeMockCatchupSupervisor(remoteActorTry = Success(remote.ref))

      underTest ! InitiateCatchup(1L)

      remote.expectMsgClass(classOf[GetLogSubrange])
    }
    describe("for successful requests with a complete subrange") {
      it("should forward the message on to the parent") {
        val parentProbe = TestProbe()
        val underTest = makeMockCatchupSupervisor(parent = parentProbe.ref)
        val mockSubrange = mock[CompleteSubrange]

        underTest ! InitiateCatchup(1L)
        underTest ! CatchupRequestSucceeded(mockSubrange)

        parentProbe.expectMsg(mockSubrange)
      }
      it("should update window for Slow Start phase") {
        val underTest = makeMockCatchupSupervisor(startingSSThresh = 100)
        underTest.underlyingActor.window = 20

        val mockSubrange = mock[CompleteSubrange]

        underTest ! InitiateCatchup(1L)
        underTest ! CatchupRequestSucceeded(mockSubrange)

        assert(40 === underTest.underlyingActor.window)
      }
      it("should update window for Congestion Avoidance phase") {
        val underTest = makeMockCatchupSupervisor(startingSSThresh = 100)
        underTest.underlyingActor.window = 120

        val mockSubrange = mock[CompleteSubrange]

        underTest ! InitiateCatchup(1L)
        underTest ! CatchupRequestSucceeded(mockSubrange)

        assert(122 === underTest.underlyingActor.window)
      }
      it("should not increase the window beyond maxWindowSize") {
        val underTest = makeMockCatchupSupervisor()
        underTest.underlyingActor.window = 1000

        val mockSubrange = mock[CompleteSubrange]

        underTest ! InitiateCatchup(1L)
        underTest ! CatchupRequestSucceeded(mockSubrange)

        assert(1000 === underTest.underlyingActor.window)
      }
    }
    describe("for successful requests with an empty subrange") {
      it("should forward the message on to the parent") {
        val parentProbe = TestProbe()
        val underTest = makeMockCatchupSupervisor(parent = parentProbe.ref)

        underTest ! InitiateCatchup(1L)
        underTest ! CatchupRequestSucceeded(EmptySubrange)

        parentProbe.expectMsg(EmptySubrange)
      }
      it("should leave window and ssthresh unchanged") {
        val underTest = makeMockCatchupSupervisor(startingSSThresh = 100)
        underTest.underlyingActor.window = 20

        underTest ! InitiateCatchup(1L)
        underTest ! CatchupRequestSucceeded(EmptySubrange)

        assert(20 === underTest.underlyingActor.window)
        assert(100 === underTest.underlyingActor.ssthresh)
      }
    }
    describe("for successful requests with an incomplete subrange") {
      it("should forward the message on to the parent") {
        val parentProbe = TestProbe()
        val underTest = makeMockCatchupSupervisor(parent = parentProbe.ref)
        val mockSubrange = mock[PartialSubrange]

        underTest ! InitiateCatchup(1L)
        underTest ! CatchupRequestSucceeded(mockSubrange)

        parentProbe.expectMsg(mockSubrange)
      }
      it("should leave window and ssthresh unchanged") {
        val underTest = makeMockCatchupSupervisor(startingSSThresh = 100)
        underTest.underlyingActor.window = 20

        val mockSubrange = mock[PartialSubrange]

        underTest ! InitiateCatchup(1L)
        underTest ! CatchupRequestSucceeded(mockSubrange)

        assert(20 === underTest.underlyingActor.window)
        assert(100 === underTest.underlyingActor.ssthresh)
      }
    }
    describe("for a failed catchup request") {
      it("should send nothing to the parent") {
        val parentProbe = TestProbe()
        val underTest = makeMockCatchupSupervisor(parent = parentProbe.ref)

        underTest ! InitiateCatchup(1L)
        underTest ! CatchupRequestFailed

        parentProbe.expectNoMsg()
      }
      it("should reduce window and ssthresh") {
        val underTest = makeMockCatchupSupervisor()
        underTest.underlyingActor.window = 50
        underTest.underlyingActor.ssthresh = 100

        underTest ! InitiateCatchup(1L)
        underTest ! CatchupRequestFailed

        assert(1 === underTest.underlyingActor.window)
        assert(25 === underTest.underlyingActor.ssthresh)
      }
    }

    it("should request the next subrange upon receiving a ContinueCatchup request while in catchup mode") {
      val remote = TestProbe()
      val underTest = makeMockCatchupSupervisor(remoteActorTry = Success(remote.ref))

      underTest.underlyingActor.context.become(underTest.underlyingActor.catchup(remote.ref))

      underTest ! ContinueCatchup(1L)

      remote.expectMsg(GetLogSubrange(1L, 2L))
    }
    it("should ignore InitiateCatchup requests if it's currently in catchup mode") {
      val remote = TestProbe()
      val underTest = makeMockCatchupSupervisor(remoteActorTry = Success(remote.ref))

      underTest.underlyingActor.context.become(underTest.underlyingActor.catchup(remote.ref))

      underTest ! InitiateCatchup(1L)

      remote.expectNoMsg()
    }
    it("should leave catchup mode and then be able to re-initiate catchup mode after receiving a StopCatchup") {
      val remote = TestProbe()
      val underTest = makeMockCatchupSupervisor(remoteActorTry = Success(remote.ref))

      underTest.underlyingActor.context.become(underTest.underlyingActor.catchup(remote.ref))

      underTest ! StopCatchup
      underTest ! InitiateCatchup(1L)

      remote.expectMsg(GetLogSubrange(1L, 2L))
    }
  }
}
