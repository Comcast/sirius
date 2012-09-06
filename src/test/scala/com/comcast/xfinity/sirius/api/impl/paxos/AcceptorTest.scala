package com.comcast.xfinity.sirius.api.impl.paxos

import akka.actor.ActorSystem
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.testkit.{ TestProbe, TestActorRef }
import com.comcast.xfinity.sirius.api.impl.Delete
import scala.collection.immutable.SortedMap
import scala.collection.JavaConverters._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import java.util.{TreeMap => JTreeMap}

@RunWith(classOf[JUnitRunner])
class AcceptorTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("AcceptorTest")

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("An Acceptor") {
    it ("must start off with a clean slate") {
      val acceptor = TestActorRef(new Acceptor(1))
      assert(acceptor.underlyingActor.ballotNum === Ballot.empty)
      assert(acceptor.underlyingActor.accepted === new JTreeMap[Long, PValue]())
    }

    describe("in response to Phase1A") {
      it ("must retain its ballotNum and respond appropriately if the incoming Ballot " +
          "is lesser than or equal to its own") {
        val acceptor = TestActorRef(new Acceptor(1))
        val ballotNum = Ballot(1, "a")
        val accepted = new JTreeMap[Long,PValue]();
        accepted.put(1L,PValue(ballotNum, 1, Command(null, 1, Delete("1"))))


        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted.clone().asInstanceOf[JTreeMap[Long, PValue]]

        val scoutProbe = TestProbe()
        val replyAs = TestProbe().ref
        acceptor ! Phase1A(scoutProbe.ref, Ballot(0, "a"), replyAs,0)
        scoutProbe.expectMsg(Phase1B(replyAs, ballotNum, accepted.values.asScala.toSet))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(acceptor.underlyingActor.accepted === accepted)

        acceptor ! Phase1A(scoutProbe.ref, ballotNum, replyAs,0)
        scoutProbe.expectMsg(Phase1B(replyAs, ballotNum, accepted.values.asScala.toSet))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(acceptor.underlyingActor.accepted === accepted)
      }

      it ("must update its ballotNum and respond appropriately if the incoming " +
          "Ballot is greater than its own") {
        val acceptor = TestActorRef(new Acceptor(1))
        val ballotNum = Ballot(1, "a")
        val accepted = new JTreeMap[Long,PValue]();
        accepted.put(1L,PValue(ballotNum, 1, Command(null, 1, Delete("1"))))

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted.clone().asInstanceOf[JTreeMap[Long,PValue]]

        val scoutProbe = TestProbe()
        val replyAs = TestProbe().ref
        val biggerBallotNum = Ballot(2, "a")
        acceptor ! Phase1A(scoutProbe.ref, biggerBallotNum, replyAs,0)
        scoutProbe.expectMsg(Phase1B(replyAs, biggerBallotNum, accepted.values.asScala.toSet))
        assert(acceptor.underlyingActor.ballotNum === biggerBallotNum)
        assert(acceptor.underlyingActor.accepted === accepted)
      }
      it("must trim the set of accepted decisions sent back to the leader based on latest decided slot"){
        val acceptor = TestActorRef(new Acceptor(1))
        val ballotNum = Ballot(1, "a")

        val accepted = new JTreeMap[Long,PValue]();
        accepted.put(1L,PValue(ballotNum, 1, Command(null, 1, Delete("1"))))
        accepted.put(2L,PValue(ballotNum,1,Command(null,1,Delete("2"))))
        accepted.put(3L,PValue(ballotNum,1,Command(null,1,Delete("3"))))
        accepted.put(4L,PValue(ballotNum,1,Command(null,1,Delete("something"))))

        val trimmedAccepted = new JTreeMap[Long,PValue]()
        trimmedAccepted.put(3L,PValue(ballotNum,1,Command(null,1,Delete("3"))))
        trimmedAccepted.put(4L,PValue(ballotNum,1,Command(null,1,Delete("something"))))

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted.clone().asInstanceOf[JTreeMap[Long, PValue]]

        val scoutProbe = TestProbe()
        val replyAs = TestProbe().ref
        acceptor ! Phase1A(scoutProbe.ref, ballotNum, replyAs, 2L)
        scoutProbe.expectMsg(Phase1B(replyAs, ballotNum, trimmedAccepted.values.asScala.toSet))
        assert(acceptor.underlyingActor.accepted === accepted)
      }

      it("doesn't trim accepted if decidedSlots are not in accepted")    {
        val acceptor = TestActorRef(new Acceptor(1))
        val ballotNum = Ballot(1, "a")

        val accepted = new JTreeMap[Long,PValue]();
        accepted.put(2L,PValue(ballotNum, 1, Command(null, 1, Delete("1"))))
        accepted.put(3L,PValue(ballotNum,1,Command(null,1,Delete("2"))))

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted

        val scoutProbe = TestProbe()
        val replyAs = TestProbe().ref
        acceptor ! Phase1A(scoutProbe.ref, ballotNum, replyAs, 1L)
        scoutProbe.expectMsg(Phase1B(replyAs, ballotNum, accepted.values.asScala.toSet))
        assert(acceptor.underlyingActor.accepted === accepted)
      }
    }

    describe("in response to Phase2A") {
      it ("must not update its state if the incoming Ballot is outdated") {
        val acceptor = TestActorRef(new Acceptor(1))
        val ballotNum = Ballot(1, "a")
        val accepted = new JTreeMap[Long,PValue]();
        accepted.put(1L,PValue(ballotNum, 1, Command(null, 1, Delete("1"))))

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted.clone().asInstanceOf[JTreeMap[Long,PValue]]

        val commanderProbe = TestProbe()
        val replyAs = TestProbe().ref
        acceptor ! Phase2A(commanderProbe.ref, PValue(Ballot(0, "a"), 1, Command(null, 1, Delete("1"))), replyAs)
        commanderProbe.expectMsg(Phase2B(replyAs, ballotNum))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(acceptor.underlyingActor.accepted === accepted)
      }

      it ("must update its ballotNum and accepted PValues, choosing the one with the most recent ballot, " +
          "if the incoming ballotNum is greater than or equal to its own") {
        val acceptor = TestActorRef(new Acceptor(1))
        val ballotNum = Ballot(1, "a")
        val accepted = new JTreeMap[Long,PValue]();
        accepted.put(1L,PValue(ballotNum, 1, Command(null, 1, Delete("1"))))

        acceptor.underlyingActor.ballotNum = ballotNum

        acceptor.underlyingActor.accepted = accepted.clone().asInstanceOf[JTreeMap[Long,PValue]]

        val commanderProbe = TestProbe()
        val replyAs = TestProbe().ref

        val newPValue1 = PValue(ballotNum, 2, Command(null, 2, Delete("3")))
        acceptor ! Phase2A(commanderProbe.ref, newPValue1, replyAs)
        commanderProbe.expectMsg(Phase2B(replyAs, ballotNum))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        accepted.put(2L,newPValue1)
        assert(acceptor.underlyingActor.accepted === accepted)

        val biggerBallot = Ballot(2, "a")
        val newPValue2 = PValue(biggerBallot, 3, Command(null, 3, Delete("4")))
        acceptor ! Phase2A(commanderProbe.ref, newPValue2, replyAs)
        commanderProbe.expectMsg(Phase2B(replyAs, biggerBallot))
        assert(acceptor.underlyingActor.ballotNum === biggerBallot)
        accepted.put(3L,newPValue2)
        assert(acceptor.underlyingActor.accepted === accepted)

        val evenBiggerBallot = Ballot(3, "a")
        val newPValue3 = PValue(evenBiggerBallot, 3, Command(null, 3, Delete("5")))
        acceptor ! Phase2A(commanderProbe.ref, newPValue3, replyAs)
        commanderProbe.expectMsg(Phase2B(replyAs, evenBiggerBallot))
        assert(acceptor.underlyingActor.ballotNum === evenBiggerBallot)
        accepted.put(3L,newPValue3)
        assert(acceptor.underlyingActor.accepted === accepted)
      }

      it ("must ignore the message if the slot is below its dignity") {
        val acceptor = TestActorRef(new Acceptor(5))
        val commanderProbe = TestProbe()

        val unnacceptablePval = PValue(Ballot(1, "a"), 4, Command(null, 1, Delete("3")))

        intercept[MatchError] {
          acceptor.underlyingActor.receive(Phase2A(commanderProbe.ref, unnacceptablePval, TestProbe().ref))
        }
      }
    }

    describe("in response to a Reap message") {
      it ("must truncate the collection of pvalues it contains") {
        val acceptor = TestActorRef(new Acceptor(1))

        val now = System.currentTimeMillis()
        val keepers = SortedMap[Long, PValue](
          4L -> PValue(Ballot(3, "b"), 4, Command(null, now - 1000, Delete("3"))),
          5L -> PValue(Ballot(3, "b"), 5, Command(null, 1L, Delete("Z"))),
          6L -> PValue(Ballot(4, "b"), 6, Command(null, now, Delete("R")))
        )

        val accepted = new JTreeMap[Long,PValue]()

        accepted.putAll((SortedMap(
          1L -> PValue(Ballot(1, "a"), 1, Command(null, (now - Acceptor.reapWindow - 100L), Delete("1"))),
          2L -> PValue(Ballot(2, "b"), 2, Command(null, (now- Acceptor.reapWindow -105L), Delete("2")))
        ) ++ keepers).asJava)

        acceptor.underlyingActor.accepted = accepted

        acceptor ! Acceptor.Reap

        assert(keepers.asJava === acceptor.underlyingActor.accepted)
        assert(3 === acceptor.underlyingActor.accepted.size)
        assert(3L === acceptor.underlyingActor.lowestAcceptableSlotNumber)
      }

      it ("must not update its lowestAcceptableSlotNumber if nothing is reaped") {
        val acceptor = TestActorRef(new Acceptor(10))

        acceptor.underlyingActor.accepted = new JTreeMap[Long, PValue]()
        acceptor ! Acceptor.Reap
        assert(10 === acceptor.underlyingActor.lowestAcceptableSlotNumber)

        acceptor.underlyingActor.accepted = new JTreeMap[Long,PValue]()
        acceptor.underlyingActor.accepted.putAll(
        SortedMap[Long, PValue](
          1L -> PValue(Ballot(1, "A"), 11, Command(null, System.currentTimeMillis(), Delete("Z")))
        ).asJava)
        acceptor ! Acceptor.Reap
        assert(10 === acceptor.underlyingActor.lowestAcceptableSlotNumber)
      }
    }
  }
}