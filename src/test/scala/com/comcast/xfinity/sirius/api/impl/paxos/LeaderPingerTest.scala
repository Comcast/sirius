package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import akka.testkit.{TestActorRef, TestProbe}
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderPinger.{Pong, Ping}
import akka.actor._
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.{LeaderPong, LeaderGone, DifferentLeader}
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.LeaderPong
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderWatcher.DifferentLeader
import com.comcast.xfinity.sirius.api.impl.paxos.LeaderPinger.Pong
import scala.Some

class LeaderPingerTest extends NiceTest {

  implicit val actorSystem = ActorSystem("LeaderPingerTest")

  def makePinger(ballot: Ballot = Ballot(1, TestProbe().ref.path.toString),
                 replyTo: ActorRef = TestProbe().ref,
                 pingReceiveTimeout: Int = 2000) = {
    TestActorRef(new LeaderPinger(ballot, replyTo, pingReceiveTimeout))
  }


  describe ("upon instantiation") {
    it ("should send the expectedLeader a Ping message") {
      val leaderProbe = TestProbe()
      makePinger(ballot = Ballot(1, leaderProbe.ref.path.toString))

      leaderProbe.expectMsg(Ping)
    }
  }

  describe ("upon receiving a Pong message") {
    it ("should stop quietly if a Pong with the expected ballot is received") {
      val terminationProbe = TestProbe()
      val leaderProbe = TestProbe()
      val expectedBallot = Ballot(1, leaderProbe.ref.path.toString)
      val replyTo = TestProbe()
      val underTest = makePinger(expectedBallot, replyTo.ref)
      terminationProbe.watch(underTest)

      underTest ! Pong(Some(expectedBallot))

      replyTo.expectMsgClass(classOf[LeaderPong])
      terminationProbe.expectMsgClass(classOf[Terminated])
    }

    it ("should inform replyTo that there is a different leader " +
      "if a Pong with an unexpected ballot is received") {
      val terminationProbe = TestProbe()
      val leaderProbe = TestProbe()
      val expectedBallot = Ballot(1, leaderProbe.ref.path.toString)
      val differentBallot = Ballot(2, leaderProbe.ref.path.toString)

      val replyTo = TestProbe()
      val underTest = makePinger(expectedBallot, replyTo.ref)
      terminationProbe.watch(underTest)

      underTest ! Pong(Some(differentBallot))

      replyTo.expectMsg(DifferentLeader(differentBallot))
      terminationProbe.expectMsgClass(classOf[Terminated])
    }

    it ("should inform replyTo that the leader is gone if a Pong with " +
      "no ballot is received") {
      val terminationProbe = TestProbe()
      val leaderProbe = TestProbe()
      val expectedBallot = Ballot(1, leaderProbe.ref.path.toString)

      val replyTo = TestProbe()
      val underTest = makePinger(expectedBallot, replyTo.ref)
      terminationProbe.watch(underTest)

      underTest ! Pong(None)

      replyTo.expectMsg(LeaderGone)
      terminationProbe.expectMsgClass(classOf[Terminated])
    }

    it ("should inform replyTo that the leader is gone if its Ping times out") {
      val replyToProbe = TestProbe()
      val underTest = makePinger(replyTo = replyToProbe.ref)

      underTest ! ReceiveTimeout

      replyToProbe.expectMsg(100 milliseconds, LeaderGone)
    }

    it ("should receive a ReceiveTimeout and act correctly if it waits too long") {
      val replyToProbe = TestProbe()
      makePinger(replyTo = replyToProbe.ref, pingReceiveTimeout = 50)

      replyToProbe.expectMsg(200 milliseconds, LeaderGone)
    }
  }

}
