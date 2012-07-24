package com.comcast.xfinity.sirius.api.impl.paxos

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.testkit.{TestProbe}
import akka.actor.{Props, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import akka.util.duration._
import com.comcast.xfinity.sirius.api.impl.{Put, Delete}

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

    it ("must properly translate a Submit message to a Request and forward it " +
        "into the system") {
      val replicaProbe = TestProbe()

      val paxosSup = actorSystem.actorOf(Props(
          new PaxosSup with PaxosSup.ChildProvider {
            val leader = TestProbe().ref
            val acceptor = TestProbe().ref
            val replica = replicaProbe.ref
          }))

      val senderProbe = TestProbe()

      val delete = Delete("a")
      senderProbe.send(paxosSup, PaxosSup.Submit(delete))
      replicaProbe.receiveOne(1 second) match {
        case Request(Command(sender, ts, req)) =>
          assert(senderProbe.ref === sender)
          // accept some tolerance on timestamp
          assert(System.currentTimeMillis() - ts < 5000)
          assert(req === delete)
      }

      val put = Put("a", "bc".getBytes)
      senderProbe.send(paxosSup, PaxosSup.Submit(put))
      replicaProbe.receiveOne(1 second) match {
        case Request(Command(sender, ts, req)) =>
          assert(senderProbe.ref === sender)
          // accept some tolerance on timestamp
          assert(System.currentTimeMillis() - ts < 5000)
          assert(req === put)
      }
    }
  }
}