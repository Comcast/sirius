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
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.Helpers.base64
import scala.concurrent.duration._
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.LogSubrange
import akka.actor._
import akka.actor.Terminated
import com.comcast.xfinity.sirius.api.impl.state.SiriusPersistenceActor.GetLogSubrange
import java.util.concurrent.atomic.AtomicLong

class CatchupActorTest extends NiceTest {
  implicit val actorSystem = ActorSystem("CatchupActorTest")

  val atomicLong = new AtomicLong
  def makeCatchupActor(parent: ActorRef = TestProbe().ref, remote: ActorRef = TestProbe().ref): TestActorRef[CatchupActor] = {
    // in order to specify the parent, we also have to specify a name for this actor. using akka's general approach anyway.
    val name = "$" + base64(atomicLong.getAndIncrement)
    TestActorRef(Props(classOf[CatchupActor], remote, 1L, 100L, 2.minutes), parent, name)
  }

  describe("CatchupActor") {
    it("must fire off a GetLogSubrange request when it starts") {
      val remote = TestProbe()
      makeCatchupActor(remote = remote.ref)

      remote.expectMsg(GetLogSubrange(1, 100))
    }

    it("must report success to its parent and shut down") {
      val parent = TestProbe()
      val mockSubrange = mock[LogSubrange]

      val underTest = makeCatchupActor(parent = parent.ref)
      parent.watch(underTest)

      underTest ! mockSubrange

      parent.expectMsg(CatchupRequestSucceeded(mockSubrange))
      parent.expectMsgClass(classOf[Terminated])
    }

    it("must report failure to its parent") {
      val parent = TestProbe()

      val underTest = makeCatchupActor(parent = parent.ref)
      parent.watch(underTest)

      underTest ! ReceiveTimeout

      parent.expectMsg(CatchupRequestFailed)
      parent.expectMsgClass(classOf[Terminated])
    }
  }
}
