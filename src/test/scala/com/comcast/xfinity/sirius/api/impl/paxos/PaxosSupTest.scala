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

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.testkit.TestProbe
import akka.actor.{ActorRef, ActorContext, Props, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages._
import scala.concurrent.duration._
import com.comcast.xfinity.sirius.api.impl.{Put, Delete}

class PaxosSupTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("PaxosSupTest")

  def createPaxosSup(leader: ActorRef = TestProbe().ref,
                     acceptor: ActorRef = TestProbe().ref,
                     replica: ActorRef = TestProbe().ref)
                    (implicit actorSystem: ActorSystem): ActorRef = {
    val childProvider = new PaxosSup.ChildProvider(null, 0L, null, null) {
      override def createLeader()(implicit context: ActorContext) = leader
      override def createAcceptor()(implicit context: ActorContext) = acceptor
      override def createReplica(leader: ActorRef)(implicit context: ActorContext) = replica
    }
    actorSystem.actorOf(Props(new PaxosSup(childProvider)))
  }

  override def afterAll() {
    actorSystem.shutdown()
  }

  describe("A PaxosSup") {
    it ("must properly forward messages to its children") {
      val leaderProbe = TestProbe()
      val acceptorProbe = TestProbe()
      val replicaProbe = TestProbe()

      val paxosSup = createPaxosSup(leaderProbe.ref, acceptorProbe.ref, replicaProbe.ref)

      val senderProbe = TestProbe()

      val decision = Decision(1, Command(null, 1, Delete("2")))
      senderProbe.send(paxosSup, decision)
      replicaProbe.expectMsg(decision)
      assert(senderProbe.ref === replicaProbe.lastSender)

      val propose = Propose(1, Command(null, 1, Delete("2")))
      senderProbe.send(paxosSup, propose)
      leaderProbe.expectMsg(propose)
      assert(senderProbe.ref === leaderProbe.lastSender)

      val decisionHint = DecisionHint(1L)
      senderProbe.send(paxosSup, decisionHint)
      replicaProbe.expectMsg(decisionHint)
      assert(senderProbe.ref === replicaProbe.lastSender)

      val phase1A = Phase1A(senderProbe.ref, Ballot(1, "a"), senderProbe.ref,1L)
      senderProbe.send(paxosSup, phase1A)
      acceptorProbe.expectMsg(phase1A)
      assert(senderProbe.ref === acceptorProbe.lastSender)

      val phase2A = Phase2A(senderProbe.ref, PValue(Ballot(1, "a"), 1,
                            Command(null, 1, Delete("2"))), senderProbe.ref)
      senderProbe.send(paxosSup, phase2A)
      acceptorProbe.expectMsg(phase2A)
      assert(senderProbe.ref === acceptorProbe.lastSender)
    }

    it ("must properly translate a NonCommutativeSiriusRequest" +
        " to a Request and forward it into the system") {
      val replicaProbe = TestProbe()

      val paxosSup = createPaxosSup(replica = replicaProbe.ref)

      val senderProbe = TestProbe()

      val delete = Delete("a")
      senderProbe.send(paxosSup, delete)
      replicaProbe.receiveOne(1 second) match {
        case Request(Command(sender, ts, req)) =>
          assert(senderProbe.ref === sender)
          // accept some tolerance on timestamp
          assert(System.currentTimeMillis() - ts < 5000)
          assert(req === delete)
      }

      val put = Put("a", "bc".getBytes)
      senderProbe.send(paxosSup, put)
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
