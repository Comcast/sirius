package com.comcast.xfinity.sirius.api.impl.paxos

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.testkit.{TestProbe, TestKit, ImplicitSender}
import akka.actor.{Props, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import com.comcast.xfinity.sirius.api.impl.Delete

class PaxosSupTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("PaxosSupTest")

  override def afterAll {
    actorSystem.shutdown()
  }

  describe("A PaxosSup") {
    it ("must properly forward messages to its children") {
      val leaderProbe = TestProbe()
      val acceptorProbe = TestProbe()
      val replicaProbe = TestProbe()

      val paxosSup = actorSystem.actorOf(Props(
          new PaxosSup with PaxosSup.ChildProvider {
            val leader = leaderProbe.ref
            val acceptor = acceptorProbe.ref
            val replica = replicaProbe.ref
          }))

      val senderProbe = TestProbe()

      val request = Request(Command(null, 1, Delete("1")))
      senderProbe.send(paxosSup, request)
      replicaProbe.expectMsg(request)
      assert(senderProbe.ref === replicaProbe.lastSender)

      val decision = Decision(1, Command(null, 1, Delete("2")))
      senderProbe.send(paxosSup, decision)
      replicaProbe.expectMsg(decision)
      assert(senderProbe.ref === replicaProbe.lastSender)

      val propose = Propose(1, Command(null, 1, Delete("2")))
      senderProbe.send(paxosSup, propose)
      leaderProbe.expectMsg(propose)
      assert(senderProbe.ref === leaderProbe.lastSender)

      val phase1A = Phase1A(senderProbe.ref, Ballot(1, "a"))
      senderProbe.send(paxosSup, phase1A)
      acceptorProbe.expectMsg(phase1A)
      assert(senderProbe.ref === acceptorProbe.lastSender)

      val phase2A = Phase2A(senderProbe.ref, PValue(Ballot(1, "a"), 1, Command(null, 1, Delete("2"))))
      senderProbe.send(paxosSup, phase2A)
      acceptorProbe.expectMsg(phase2A)
      assert(senderProbe.ref === acceptorProbe.lastSender)
    }
  }
}