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

import akka.actor.ActorSystem
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.testkit.{ TestProbe, TestActorRef }
import com.comcast.xfinity.sirius.api.impl.Delete
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import java.util.{TreeMap => JTreeMap}
import com.comcast.xfinity.sirius.util.RichJTreeMap
import com.comcast.xfinity.sirius.api.SiriusConfiguration

@RunWith(classOf[JUnitRunner])
class AcceptorTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("AcceptorTest")
  val config = new SiriusConfiguration
  val reapWindow = 10 * 60 * 1000L
  val reapFreqSecs = 30

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("An Acceptor") {
    it ("must start off with a clean slate") {
      val acceptor = TestActorRef(new Acceptor(1, reapWindow, reapFreqSecs, config))
      assert(acceptor.underlyingActor.ballotNum === Ballot.empty)
      assert(acceptor.underlyingActor.accepted === new JTreeMap[Long, PValue]())
    }

    describe("in response to Phase1A") {
      it ("must retain its ballotNum and respond appropriately if the incoming Ballot " +
          "is lesser than or equal to its own") {
        val acceptor = TestActorRef(new Acceptor(1, reapWindow, reapFreqSecs, config))
        val ballotNum = Ballot(1, "a")
        val accepted = RichJTreeMap(
          1L -> (1L, PValue(ballotNum, 1, Command(null, 1, Delete("1"))))
        )

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted.clone.asInstanceOf[RichJTreeMap[Long, (Long, PValue)]]

        val scoutProbe = TestProbe()
        val replyAs = TestProbe().ref
        acceptor ! Phase1A(scoutProbe.ref, Ballot(0, "a"), replyAs, 0)
        scoutProbe.expectMsg(Phase1B(replyAs, ballotNum, Set(PValue(ballotNum, 1, Command(null, 1, Delete("1"))))))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(acceptor.underlyingActor.accepted === accepted)

        acceptor ! Phase1A(scoutProbe.ref, ballotNum, replyAs, 0)
        scoutProbe.expectMsg(Phase1B(replyAs, ballotNum, Set(PValue(ballotNum, 1, Command(null, 1, Delete("1"))))))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(acceptor.underlyingActor.accepted === accepted)
      }

      it ("must update its ballotNum and respond appropriately if the incoming " +
          "Ballot is greater than its own") {
        val acceptor = TestActorRef(new Acceptor(1, reapWindow, reapFreqSecs, config))
        val ballotNum = Ballot(1, "a")
        val accepted = RichJTreeMap(
          1L -> (1L, PValue(ballotNum, 1, Command(null, 1, Delete("1"))))
        )

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted.clone.asInstanceOf[RichJTreeMap[Long, (Long, PValue)]]

        val scoutProbe = TestProbe()
        val replyAs = TestProbe().ref
        val biggerBallotNum = Ballot(2, "a")
        acceptor ! Phase1A(scoutProbe.ref, biggerBallotNum, replyAs, 0)
        scoutProbe.expectMsg(Phase1B(replyAs, biggerBallotNum, Set(PValue(ballotNum, 1, Command(null, 1, Delete("1"))))))
        assert(acceptor.underlyingActor.ballotNum === biggerBallotNum)
        assert(acceptor.underlyingActor.accepted === accepted)
      }

      it("must trim the set of accepted decisions sent back to the leader based on latest decided slot"){
        val acceptor = TestActorRef(new Acceptor(1, reapWindow, reapFreqSecs, config))
        val ballotNum = Ballot(1, "a")

        val slot1sPValue = PValue(ballotNum, 1, Command(null, 1, Delete("1")))
        val slot2sPValue = PValue(ballotNum, 1, Command(null, 1, Delete("2")))
        val slot3sPValue = PValue(ballotNum, 1, Command(null, 1, Delete("3")))
        val slot4sPValue = PValue(ballotNum, 1, Command(null, 1, Delete("something")))
        val accepted = RichJTreeMap(
          1L -> (1L, slot1sPValue),
          2L -> (1L, slot2sPValue),
          3L -> (1L, slot3sPValue),
          4L -> (1L, slot4sPValue)
        )

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted.clone.asInstanceOf[RichJTreeMap[Long, (Long, PValue)]]

        val scoutProbe = TestProbe()
        val replyAs = TestProbe().ref

        acceptor ! Phase1A(scoutProbe.ref, ballotNum, replyAs, 2L)
        scoutProbe.expectMsg(Phase1B(replyAs, ballotNum, Set(slot3sPValue, slot4sPValue)))
        assert(acceptor.underlyingActor.accepted === accepted)

        acceptor ! Phase1A(scoutProbe.ref, ballotNum, replyAs, 0L)
        scoutProbe.expectMsg(Phase1B(replyAs, ballotNum, Set(slot1sPValue, slot2sPValue, slot3sPValue, slot4sPValue)))
      }
    }

    describe("in response to Phase2A") {
      it ("must not update its state if the incoming Ballot is outdated") {
        val acceptor = TestActorRef(new Acceptor(1, reapWindow, reapFreqSecs, config))
        val ballotNum = Ballot(1, "a")
        val accepted = RichJTreeMap(
          1L -> (1L,PValue(ballotNum, 1, Command(null, 1, Delete("1"))))
        )

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted.clone.asInstanceOf[RichJTreeMap[Long, (Long, PValue)]]

        val commanderProbe = TestProbe()
        val replyAs = TestProbe().ref
        acceptor ! Phase2A(commanderProbe.ref, PValue(Ballot(0, "a"), 1, Command(null, 1, Delete("1"))), replyAs)
        commanderProbe.expectMsg(Phase2B(replyAs, ballotNum))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(acceptor.underlyingActor.accepted === accepted)
      }

      it ("must update its ballotNum and accepted PValues, choosing the one with the most recent ballot, " +
          "if the incoming ballotNum is greater than or equal to its own") {
        val acceptor = TestActorRef(new Acceptor(1, reapWindow, reapFreqSecs, config))
        val ballotNum = Ballot(1, "a")
        val accepted = RichJTreeMap(
          1L -> (1L, PValue(ballotNum, 1, Command(null, 1, Delete("1"))))
        )

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted

        val commanderProbe = TestProbe()
        val replyAs = TestProbe().ref

        val newPValue1 = PValue(ballotNum, 2, Command(null, 2, Delete("3")))
        acceptor ! Phase2A(commanderProbe.ref, newPValue1, replyAs)
        commanderProbe.expectMsg(Phase2B(replyAs, ballotNum))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(newPValue1 === acceptor.underlyingActor.accepted.get(2L)._2)

        val biggerBallot = Ballot(2, "a")
        val newPValue2 = PValue(biggerBallot, 3, Command(null, 3, Delete("4")))
        acceptor ! Phase2A(commanderProbe.ref, newPValue2, replyAs)
        commanderProbe.expectMsg(Phase2B(replyAs, biggerBallot))
        assert(acceptor.underlyingActor.ballotNum === biggerBallot)
        assert(newPValue2 === acceptor.underlyingActor.accepted.get(3L)._2)

        val evenBiggerBallot = Ballot(3, "a")
        val newPValue3 = PValue(evenBiggerBallot, 3, Command(null, 3, Delete("5")))
        acceptor ! Phase2A(commanderProbe.ref, newPValue3, replyAs)
        commanderProbe.expectMsg(Phase2B(replyAs, evenBiggerBallot))
        assert(acceptor.underlyingActor.ballotNum === evenBiggerBallot)
        assert(newPValue3 === acceptor.underlyingActor.accepted.get(3L)._2)
      }

      it ("must ignore the message if the slot is below its dignity") {
        val acceptor = TestActorRef(new Acceptor(5, reapWindow, reapFreqSecs, config))
        val commanderProbe = TestProbe()

        val unnacceptablePval = PValue(Ballot(1, "a"), 4, Command(null, 1, Delete("3")))

        intercept[MatchError] {
          acceptor.underlyingActor.receive(Phase2A(commanderProbe.ref, unnacceptablePval, TestProbe().ref))
        }
      }
    }

    describe("in response to a Reap message") {
      it ("must truncate the collection of pvalues it contains") {
        val reapWindow = 50 * 60 * 1000L
        val acceptor = TestActorRef(new Acceptor(1, reapWindow, reapFreqSecs, config))

        val now = System.currentTimeMillis()

        val keepers = RichJTreeMap(
          4L -> (now - 1000, PValue(Ballot(3, "b"), 4, Command(null, 1L, Delete("3")))),
          5L -> (1L, PValue(Ballot(3, "b"), 5, Command(null, 1L, Delete("Z")))),
          6L -> (now, PValue(Ballot(4, "b"), 6, Command(null, 1L, Delete("R"))))
        )

        val accepted = RichJTreeMap(
          1L -> (1L, PValue(Ballot(1, "a"), 1, Command(null, (now - reapWindow - 100L), Delete("1")))),
          2L -> (1L, PValue(Ballot(2, "b"), 2, Command(null, (now - reapWindow - 105L), Delete("2"))))
        )
        accepted.putAll(keepers)

        acceptor.underlyingActor.accepted = accepted

        acceptor ! Acceptor.Reap

        assert(keepers === acceptor.underlyingActor.accepted)
        assert(3 === acceptor.underlyingActor.accepted.size)
        assert(3L === acceptor.underlyingActor.lowestAcceptableSlotNumber)
      }

      it ("must not update its lowestAcceptableSlotNumber if nothing is reaped") {
        val acceptor = TestActorRef(new Acceptor(10, reapWindow, reapFreqSecs, config))

        // empty
        acceptor.underlyingActor.accepted = RichJTreeMap[Long, (Long, PValue)]()
        acceptor ! Acceptor.Reap
        assert(10 === acceptor.underlyingActor.lowestAcceptableSlotNumber)

        // non-empty
        acceptor.underlyingActor.accepted.put(
          1L, (System.currentTimeMillis,PValue(Ballot(1, "A"), 11, Command(null, 1L, Delete("Z"))))
        )
        acceptor ! Acceptor.Reap
        assert(10 === acceptor.underlyingActor.lowestAcceptableSlotNumber)
      }
    }
  }
}
