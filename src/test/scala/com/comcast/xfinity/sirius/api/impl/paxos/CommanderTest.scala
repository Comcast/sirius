package com.comcast.xfinity.sirius.api.impl.paxos

import org.scalatest.BeforeAndAfterAll

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Command
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.PValue
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.NiceTest

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.testkit.TestProbe


class CommanderTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("CommanderTest")

  // XXX: how to test ReceiveTimeout?

  describe("A Commander") {
    it ("must send a Phase2A request to all acceptors on startup") {
      val leaderProbe = TestProbe()
      val acceptorProbes = Set(TestProbe(), TestProbe(), TestProbe())
      val replicaProbes = Set[TestProbe]()
      println(acceptorProbes)
      val pvalue = PValue(Ballot(1, "a"), 1, Command(null, 1, Delete("2")))
      val commander = TestActorRef(new Commander(leaderProbe.ref,
                                   acceptorProbes.map(_.ref),
                                   replicaProbes.map(_.ref),
                                   pvalue))
      acceptorProbes.foreach(_.expectMsg(Phase2A(commander, pvalue)))
    }

    it ("must notify its leader if it is preempted with a greater ballot and exit") {
      val leaderProbe = TestProbe()
      val anAcceptorProbe = TestProbe()
      val acceptorProbes = Set(anAcceptorProbe)
      val replicaProbes = Set[TestProbe]()
      println(acceptorProbes)
      val pvalue = PValue(Ballot(1, "a"), 1, Command(null, 1, Delete("2")))
      val commander = TestActorRef(new Commander(leaderProbe.ref,
                                   acceptorProbes.map(_.ref),
                                   replicaProbes.map(_.ref),
                                   pvalue))

      val biggerBallot = Ballot(2, "b")
      commander ! Phase2B(anAcceptorProbe.ref, biggerBallot)
      leaderProbe.expectMsg(Preempted(biggerBallot))
      assert(commander.isTerminated)
    }

    it("must notify the replicas once a majority of acceptors have responded and exit") {
      val leaderProbe = TestProbe()
      val acceptorProbes = Set(TestProbe(), TestProbe(), TestProbe())
      val replicaProbes = Set(TestProbe(), TestProbe(), TestProbe())
      println(acceptorProbes)
      val pvalue = PValue(Ballot(1, "a"), 1, Command(null, 1, Delete("2")))
      val commander = TestActorRef(new Commander(leaderProbe.ref,
                                   acceptorProbes.map(_.ref),
                                   replicaProbes.map(_.ref),
                                   pvalue))

      acceptorProbes.foreach(probe => commander ! Phase2B(probe.ref, pvalue.ballot))

      replicaProbes.foreach(_.expectMsg(Decision(pvalue.slotNum, pvalue.proposal)))
      assert(commander.isTerminated)
    }
  }
}