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

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.actor.{Terminated, ActorSystem}
import akka.testkit.{TestProbe, TestActorRef}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.impl.Delete


class ScoutTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("ScoutTest")

  override def afterAll {
    actorSystem.shutdown()
  }

  // XXX: how to test ReceiveTimeout?

  describe("A Scout") {
    it ("must send a Phase1A message to all acceptors on startup, including the proper " +
        "replyAs field") {
      val leaderProbe = TestProbe()
      val acceptorProbes = Set(TestProbe(), TestProbe(), TestProbe())
      val ballot = Ballot(1, "a")
      val scout = TestActorRef(new Scout(leaderProbe.ref,
                                         acceptorProbes.map(_.ref),
                                         ballot, 1L, 2))

      acceptorProbes.foreach(
        probe => probe.expectMsg(Phase1A(scout, ballot, probe.ref, 1L))
      )
    }

    it ("must notifiy its leader if it is preempted with a greater ballot and exit") {
      val terminationProbe = TestProbe()
      val leaderProbe = TestProbe()
      val acceptorProbe = TestProbe()
      val ballot = Ballot(1, "a")
      val scout = TestActorRef(new Scout(leaderProbe.ref,
                                         Set(acceptorProbe.ref),
                                         ballot, 1L, 1))
      terminationProbe.watch(scout)

      val biggerBallot = Ballot(2, "b")
      scout ! Phase1B(acceptorProbe.ref, biggerBallot, Set[PValue]())
      leaderProbe.expectMsg(Preempted(biggerBallot))
      terminationProbe.expectMsgClass(classOf[Terminated])
    }

    // Here it is important to note that we are waiting until there are < n/2 oustanding
    // responses, so for odd numbers, this will be more than a majority
    it ("must update its pvalues with those received from acceptors when the received " +
        "ballot is equal to its own, and notify the leader once a major majority of " +
        "responses are received") {
      val terminationProbe = TestProbe()
      val leaderProbe = TestProbe()
      val anAcceptorProbe1 = TestProbe()
      val anAcceptorProbe2 = TestProbe()
      val anAcceptorProbe3 = TestProbe()
      val acceptorProbes = Set(anAcceptorProbe1, anAcceptorProbe2, anAcceptorProbe3, TestProbe())
      val ballot = Ballot(1, "a")
      val scout = TestActorRef(new Scout(leaderProbe.ref,
                                         acceptorProbes.map(_.ref),
                                         ballot, 1L, 3))
      terminationProbe.watch(scout)

      assert(Set[PValue]() === scout.underlyingActor.pvalues)

      val acceptor1sPValues = Set(PValue(Ballot(0, "a"), 1, Command(null, 1, Delete("2"))))
      scout ! Phase1B(anAcceptorProbe1.ref, ballot, acceptor1sPValues)
      leaderProbe.expectNoMsg()
      assert(acceptor1sPValues == scout.underlyingActor.pvalues)

      val acceptor2sPValues = Set(PValue(Ballot(0, "b"), 2, Command(null, 1, Delete("2"))))
      scout ! Phase1B(anAcceptorProbe2.ref, ballot, acceptor2sPValues)
      leaderProbe.expectNoMsg()

      scout ! Phase1B(anAcceptorProbe3.ref, ballot, Set[PValue]())
      leaderProbe.expectMsg(Adopted(ballot, acceptor1sPValues ++ acceptor2sPValues))
      terminationProbe.expectMsgClass(classOf[Terminated])
    }

    it ("must be able to make progress as a forever alone") {
      val terminationProbe = TestProbe()
      val leaderProbe = TestProbe()
      val acceptorProbe = TestProbe()
      val ballot = Ballot(1, "a")
      val scout = TestActorRef(new Scout(leaderProbe.ref, Set(acceptorProbe.ref), ballot, 1L, 1))
      terminationProbe.watch(scout)

      assert(Set[PValue]() === scout.underlyingActor.pvalues)

      val acceptorsPValues = Set(PValue(Ballot(0, "a"), 1, Command(null, 1, Delete("2"))))
      scout ! Phase1B(acceptorProbe.ref, ballot, acceptorsPValues)
      leaderProbe.expectMsg(Adopted(ballot, acceptorsPValues))
      terminationProbe.expectMsgClass(classOf[Terminated])
    }
  }
}
