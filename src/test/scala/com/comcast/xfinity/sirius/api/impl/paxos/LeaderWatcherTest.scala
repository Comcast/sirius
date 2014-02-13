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
package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import akka.testkit.{TestActorRef, TestProbe}
import akka.actor.{Terminated, ActorContext, ActorSystem, ActorRef}
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Preempted
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.DifferentLeader
import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.api.SiriusConfiguration

class LeaderWatcherTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("LeaderPingerTest")

  case class PingersCreated(num: Int)

  def makeWatcher(leaderToWatch: ActorRef = TestProbe().ref,
                  ballot: Ballot = Ballot(1, TestProbe().ref.path.toString),
                  pinger: ActorRef = TestProbe().ref,
                  replyTo: ActorRef = TestProbe().ref,
                  pingerCreationNotifier: ActorRef = TestProbe().ref) = {

    val childProvider = new ChildProvider(leaderToWatch, ballot, new SiriusConfiguration) {
      var pingersCreated = 0
      override def createPinger(replyTo: ActorRef)
                               (implicit context: ActorContext) = {
        pingersCreated += 1
        pingerCreationNotifier ! PingersCreated(pingersCreated)
        pinger
      }
    }
    TestActorRef(new LeaderWatcher(replyTo, childProvider, new SiriusConfiguration))
  }

  override def afterAll() {
    actorSystem.shutdown()
  }

  describe ("on instantiation") {
    it ("creates a pinger") {
      val pingerCreationNotifier = TestProbe()
      makeWatcher(pingerCreationNotifier = pingerCreationNotifier.ref)

      pingerCreationNotifier.expectMsg(PingersCreated(1))
    }
  }

  describe ("upon receiving a CheckLeader message") {
    it ("creates a pinger") {
      val pingerCreationNotifier = TestProbe()
      val watcher = makeWatcher(pingerCreationNotifier = pingerCreationNotifier.ref)

      pingerCreationNotifier.expectMsg(PingersCreated(1))
      watcher ! CheckLeader
      pingerCreationNotifier.expectMsg(PingersCreated(2))
    }
  }

  describe ("upon receiving a LeaderGone message") {
    it ("tells replyTo to seek leadership and stops") {
      val terminationProbe = TestProbe()
      val replyTo = TestProbe()
      val watcher = makeWatcher(replyTo = replyTo.ref)
      terminationProbe.watch(watcher) // who watches the watchmen?

      watcher ! LeaderGone

      replyTo.expectMsg(LeaderGone)
      terminationProbe.expectMsgClass(classOf[Terminated])
    }
  }

  describe ("upon receiving a DifferentLeader message") {
    it ("preempts replyTo with the new ballot") {
      val terminationProbe = TestProbe()
      val replyTo = TestProbe()
      val watcher = makeWatcher(replyTo = replyTo.ref)
      val newBallot = Ballot(1, TestProbe().ref.path.toString)
      terminationProbe.watch(watcher)

      watcher ! DifferentLeader(newBallot)

      replyTo.expectMsg(Preempted(newBallot))
      terminationProbe.expectMsgClass(classOf[Terminated])
    }
  }

  describe ("upon receiving a Close message") {
    it ("dies quietly") {
      val terminationProbe = TestProbe()
      val watcher = makeWatcher()
      terminationProbe.watch(watcher)

      watcher ! Close

      terminationProbe.expectMsgClass(classOf[Terminated])
    }
  }
}
