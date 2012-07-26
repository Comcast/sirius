package com.comcast.xfinity.sirius.api.impl.paxos

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.actor.ActorSystem
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
                                         ballot))

      acceptorProbes.foreach(
        probe => probe.expectMsg(Phase1A(scout, ballot, probe.ref))
      )
    }

    it ("must notifiy its leader if it is preempted with a greater ballot and exit") {
      val leaderProbe = TestProbe()
      val acceptorProbe = TestProbe()
      val ballot = Ballot(1, "a")
      val scout = TestActorRef(new Scout(leaderProbe.ref,
                                         Set(acceptorProbe.ref),
                                         ballot))

      val biggerBallot = Ballot(2, "b")
      scout ! Phase1B(acceptorProbe.ref, biggerBallot, Set[PValue]())
      leaderProbe.expectMsg(Preempted(biggerBallot))
      assert(scout.isTerminated)
    }

    // Here it is important to note that we are waiting until there are < n/2 oustanding
    // responses, so for odd numbers, this will be more than a majority
    it ("must update its pvalues with those received from acceptors when the received " +
        "ballot is equal to its own, and notify the leader once a major majority of " +
        "responses are received") {
      val leaderProbe = TestProbe()
      val anAcceptorProbe1 = TestProbe()
      val anAcceptorProbe2 = TestProbe()
      val anAcceptorProbe3 = TestProbe()
      val acceptorProbes = Set(anAcceptorProbe1, anAcceptorProbe2, anAcceptorProbe3, TestProbe())
      val ballot = Ballot(1, "a")
      val scout = TestActorRef(new Scout(leaderProbe.ref,
                                         acceptorProbes.map(_.ref),
                                         ballot))

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
      assert(scout.isTerminated)
    }
  }
}