package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderPinger.{Pong, Ping}
import akka.actor.{ReceiveTimeout, ActorRef, Props, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.{LeaderGone, DifferentLeader}

class LeaderPingerTest extends NiceTest {

  implicit val actorSystem = ActorSystem("LeaderPingerTest")

  def makePinger(ballot: Ballot = Ballot(1, TestProbe().ref.path.toString),
                 replyTo: ActorRef = TestProbe().ref) = {
    TestActorRef(new LeaderPinger(ballot, replyTo))
  }


  describe ("upon instantiation") {
    it ("should send the expectedLeader a Ping message") {
      val leaderProbe = TestProbe()
      val underTest = makePinger(ballot = Ballot(1, leaderProbe.ref.path.toString))

      leaderProbe.expectMsg(Ping)
    }
  }

  describe ("upon receiving a Pong message") {
    it ("should stop quietly if a Pong with the expected ballot is received") {
      val leaderProbe = TestProbe()
      val expectedBallot = Ballot(1, leaderProbe.ref.path.toString)
      val replyTo = TestProbe()
      val underTest = makePinger(expectedBallot, replyTo.ref)

      underTest ! Pong(Some(expectedBallot))

      replyTo.expectNoMsg()
      assert(underTest.isTerminated)
    }

    it ("should inform replyTo that there is a different leader " +
      "if a Pong with an unexpected ballot is received") {
      val leaderProbe = TestProbe()
      val expectedBallot = Ballot(1, leaderProbe.ref.path.toString)
      val differentBallot = Ballot(2, leaderProbe.ref.path.toString)

      val replyTo = TestProbe()
      val underTest = makePinger(expectedBallot, replyTo.ref)

      underTest ! Pong(Some(differentBallot))

      replyTo.expectMsg(DifferentLeader(differentBallot))
      assert(underTest.isTerminated)
    }

    it ("should inform replyTo that the leader is gone if a Pong with " +
      "no ballot is received") {
      val leaderProbe = TestProbe()
      val expectedBallot = Ballot(1, leaderProbe.ref.path.toString)

      val replyTo = TestProbe()
      val underTest = makePinger(expectedBallot, replyTo.ref)

      underTest ! Pong(None)

      replyTo.expectMsg(LeaderGone)
      assert(underTest.isTerminated)
    }

    it ("should inform replyTo that the leader is gone if its Ping times out") {
      val replyToProbe = TestProbe()
      val underTest = makePinger(replyTo = replyToProbe.ref)

      underTest ! ReceiveTimeout

      replyToProbe.expectMsg(100 milliseconds, LeaderGone)
    }
  }

}
