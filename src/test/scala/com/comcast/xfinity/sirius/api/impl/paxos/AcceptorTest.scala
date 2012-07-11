package com.comcast.xfinity.sirius.api.impl.paxos

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.testkit.{TestProbe, TestActorRef}


class AcceptorTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("AcceptorTest")

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("An Acceptor") {
    it ("must start off with a clean slate") {
      val acceptor = TestActorRef[Acceptor]
      assert(acceptor.underlyingActor.ballotNum === Ballot.empty)
      assert(acceptor.underlyingActor.accepted === Set[PValue]())
    }

    describe("in response to Phase1A") {
      it ("must retain its ballotNum and respond appropriately if the incoming Ballot is lesser than or equal to its own") {
        val acceptor = TestActorRef[Acceptor]
        val ballotNum = Ballot(1, "a")
        val accepted = Set(PValue(ballotNum, 1, Command(null, 1, 1)))

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted

        val scoutProbe = TestProbe()
        acceptor ! Phase1A(scoutProbe.ref, Ballot(0, "a"))
        scoutProbe.expectMsg(Phase1B(acceptor.getParent, ballotNum, accepted))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(acceptor.underlyingActor.accepted === accepted)

        acceptor ! Phase1A(scoutProbe.ref, ballotNum)
        scoutProbe.expectMsg(Phase1B(acceptor.getParent, ballotNum, accepted))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(acceptor.underlyingActor.accepted === accepted)
      }

      it ("must update its ballotNum and respond appropriately if the incoming " +
          "Ballot is greater than its own") {
        val acceptor = TestActorRef[Acceptor]
        val ballotNum = Ballot(1, "a")
        val accepted = Set(PValue(ballotNum, 1, Command(null, 1, 1)))

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted

        val scoutProbe = TestProbe()
        val biggerBallotNum = Ballot(2, "a")
        acceptor ! Phase1A(scoutProbe.ref, biggerBallotNum)
        scoutProbe.expectMsg(Phase1B(acceptor.getParent, biggerBallotNum, accepted))
        assert(acceptor.underlyingActor.ballotNum === biggerBallotNum)
        assert(acceptor.underlyingActor.accepted === accepted)
      }
    }

    describe("in response to Phase2A") {
      it ("must not update its state if the incoming Ballot is outdated") {
        val acceptor = TestActorRef[Acceptor]
        val ballotNum = Ballot(1, "a")
        val accepted = Set(PValue(ballotNum, 1, Command(null, 1, 1)))

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted

        val commanderProbe = TestProbe()
        acceptor ! Phase2A(commanderProbe.ref, PValue(Ballot(0, "a"), 1, Command(null, 1, 1)))
        commanderProbe.expectMsg(Phase2B(acceptor.getParent, ballotNum))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(acceptor.underlyingActor.accepted === accepted)
      }

      it ("must update its ballotNum and accepted PValues if the incoming ballotNum " +
          "is greater than or equal to its own") {
        val acceptor = TestActorRef[Acceptor]
        val ballotNum = Ballot(1, "a")
        val accepted = Set(PValue(ballotNum, 1, Command(null, 1, 1)))

        acceptor.underlyingActor.ballotNum = ballotNum
        acceptor.underlyingActor.accepted = accepted

        val commanderProbe = TestProbe()

        val newPValue1 = PValue(ballotNum, 2, Command(null, 2, 3))
        acceptor ! Phase2A(commanderProbe.ref, newPValue1)
        commanderProbe.expectMsg(Phase2B(acceptor.getParent, ballotNum))
        assert(acceptor.underlyingActor.ballotNum === ballotNum)
        assert(acceptor.underlyingActor.accepted === accepted + newPValue1)

        val biggerBallot = Ballot(2, "a")
        val newPValue2 = PValue(biggerBallot, 3, Command(null, 3, 4))
        acceptor ! Phase2A(commanderProbe.ref, newPValue2)
        commanderProbe.expectMsg(Phase2B(acceptor.getParent, biggerBallot))
        assert(acceptor.underlyingActor.ballotNum === biggerBallot)
        assert(acceptor.underlyingActor.accepted === accepted + newPValue1 + newPValue2)
      }
    }
  }
}