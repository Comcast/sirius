package com.comcast.xfinity.sirius.api.impl.paxos

import org.scalatest.BeforeAndAfterAll

import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Command
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.PValue
import com.comcast.xfinity.sirius.api.impl.Delete
import com.comcast.xfinity.sirius.NiceTest

import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import akka.actor.{Terminated, ReceiveTimeout, ActorSystem}

class CommanderTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("CommanderTest")

  // XXX: how to test ReceiveTimeout?

  describe("A Commander") {
    it ("must send a Phase2A request to all acceptors on startup") {
      val leaderProbe = TestProbe()
      val acceptorProbes = Set(TestProbe(), TestProbe(), TestProbe())
      val replicaProbes = Set[TestProbe]()
      val pvalue = PValue(Ballot(1, "a"), 1, Command(null, 1, Delete("2")))
      val commander = TestActorRef(new Commander(leaderProbe.ref,
                                   acceptorProbes.map(_.ref),
                                   replicaProbes.map(_.ref),
                                   pvalue, 2, 0))
      acceptorProbes.foreach(
        probe => probe.expectMsg(Phase2A(commander, pvalue, probe.ref))
      )
    }

    it ("must notify its leader if it is preempted with a greater ballot and exit") {
      val terminationProbe = TestProbe()
      val leaderProbe = TestProbe()
      val anAcceptorProbe = TestProbe()
      val acceptorProbes = Set(anAcceptorProbe)
      val replicaProbes = Set[TestProbe]()
      val pvalue = PValue(Ballot(1, "a"), 1, Command(null, 1, Delete("2")))
      val commander = TestActorRef(new Commander(leaderProbe.ref,
                                   acceptorProbes.map(_.ref),
                                   replicaProbes.map(_.ref),
                                   pvalue, 1, 0))
      terminationProbe.watch(commander)

      val biggerBallot = Ballot(2, "b")
      commander ! Phase2B(anAcceptorProbe.ref, biggerBallot)
      leaderProbe.expectMsg(Preempted(biggerBallot))
      terminationProbe.expectMsgClass(classOf[Terminated])
    }

    it ("must notify the replicas once a majority of acceptors have responded and exit") {
      val terminationProbe = TestProbe()
      val leaderProbe = TestProbe()
      val acceptorProbes = Set(TestProbe(), TestProbe(), TestProbe())
      val replicaProbes = Set(TestProbe(), TestProbe(), TestProbe())

      val pvalue = PValue(Ballot(1, "a"), 1, Command(null, 1, Delete("2")))
      val commander = TestActorRef(new Commander(leaderProbe.ref,
                                   acceptorProbes.map(_.ref),
                                   replicaProbes.map(_.ref),
                                   pvalue, 2, 0))
      terminationProbe.watch(commander)

      acceptorProbes.foreach(probe => commander ! Phase2B(probe.ref, pvalue.ballot))

      replicaProbes.foreach(_.expectMsg(Decision(pvalue.slotNum, pvalue.proposedCommand)))
      terminationProbe.expectMsgClass(classOf[Terminated])
    }

    it ("must be able to make progress as a forever alone") {
      val terminationProbe = TestProbe()
      val leaderProbe = TestProbe()
      val acceptorProbes = Set(TestProbe())
      val replicaProbes = Set(TestProbe())

      val pvalue = PValue(Ballot(1, "a"), 1, Command(null, 1, Delete("2")))
      val commander = TestActorRef(new Commander(leaderProbe.ref,
                                   acceptorProbes.map(_.ref),
                                   replicaProbes.map(_.ref),
                                   pvalue, 1, 0))
      terminationProbe.watch(commander)

      acceptorProbes.foreach(probe => commander ! Phase2B(probe.ref, pvalue.ballot))

      replicaProbes.foreach(_.expectMsg(Decision(pvalue.slotNum, pvalue.proposedCommand)))
      terminationProbe.expectMsgClass(classOf[Terminated])
    }

    it ("must notify its leader of the PValue and retryCount it timed out negotiating") {
      val terminationProbe = TestProbe()
      val leaderProbe = TestProbe()
      val acceptorProbes = Set(TestProbe())
      val replicaProbes = Set(TestProbe())

      val pvalue = PValue(Ballot(1, "a"), 1, Command(null, 1, Delete("2")))
      val commander = TestActorRef(new Commander(leaderProbe.ref,
                                   acceptorProbes.map(_.ref),
                                   replicaProbes.map(_.ref),
                                   pvalue, 1, 1))
      terminationProbe.watch(commander)

      commander ! ReceiveTimeout
      leaderProbe.expectMsg(Commander.CommanderTimeout(pvalue, 1))
      terminationProbe.expectMsgClass(classOf[Terminated])
    }
  }
}