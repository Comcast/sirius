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

import com.comcast.xfinity.sirius.NiceTest
import akka.agent.Agent
import akka.actor.{ActorRef, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.{Delete, Put, NonCommutativeSiriusRequest}
import scala.concurrent.duration._
import org.scalatest.BeforeAndAfterAll
import akka.testkit.{TestLatch, TestProbe}
import scala.concurrent.Await
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.Decision
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import scala.concurrent.ExecutionContext.Implicits.global


object PaxosITest {
  class TestNode(membership: MembershipHelper, decisionLatch: TestLatch)(implicit as: ActorSystem) {
    var decisions = Set[Decision]()

    val paxosSup = as.actorOf(
      PaxosSup.props(membership, 1, {
        case decision if !decisions.contains(decision) =>
          decisions += decision
          decisionLatch.countDown()
        case decision if decisions.contains(decision) =>
      }, new SiriusConfiguration)
    )

    def hasDecisionFor(req: NonCommutativeSiriusRequest): Boolean =
      decisions.exists(req == _.command.op)
  }
}

class PaxosITest extends NiceTest with BeforeAndAfterAll {

  import PaxosITest._

  implicit val as = ActorSystem("PaxosITest")

  override def afterAll {
    as.shutdown()
    as.awaitTermination()
  }

  describe("The Paxos subsystem") {
    it ("must arrive at a decision when all requests are sent to a single node, and " +
        "the initiators must be properly notified") {
      val membership = Agent(Map[String, Option[ActorRef]]())
      val membershipHelper = MembershipHelper(membership, TestProbe().ref)

      // 3 nodes x 3 requests = 9 applied decisions
      val decisionLatch = TestLatch(9)

      // Each of these TestNodes contains a paxos system and
      // set of performed decisions.  The set of decisions is
      // updated and the decisionLatch is counted down when
      // a new decision arrives
      val node1 = new TestNode(membershipHelper, decisionLatch)
      val node2 = new TestNode(membershipHelper, decisionLatch)
      val node3 = new TestNode(membershipHelper, decisionLatch)

      // with the nodes created, establish membership
      membership send (_ + ("node1" -> Some(node1.paxosSup)))
      membership send (_ + ("node2" -> Some(node2.paxosSup)))
      membership send (_ + ("node3" -> Some(node3.paxosSup)))

      // stage and send requests
      val req1 = Delete("A")
      val req2 = Put("B", "C".getBytes)
      val req3 = Delete("D")

      node1.paxosSup ! req1
      node1.paxosSup ! req2
      node1.paxosSup ! req3

      // Wait for the decision to actually be received
      // on each node, the request RequestPerformed
      // message only indicates that one node has
      // received and processed the decision
      Await.ready(decisionLatch, 7 seconds)

      // assure that all nodes have the same decisions,
      // and that all requests were decided on
      assert(node1.decisions === node2.decisions,
        "Node 1 & Node 2's decisions did not match")
      assert(node2.decisions === node3.decisions,
        "Node 2 & Node 3's decisions did not match")
      // comparing 1 and 3 isn't necessary due to the
      // transitive property

      // now check that each decision exists on each node
      List(node1, node2, node3).foreach(
        (node) => {
          assert(node.hasDecisionFor(req1))
          assert(node.hasDecisionFor(req2))
          assert(node.hasDecisionFor(req3))
        }
      )

      // TODO: throw some more in here?
    }

    // TODO: some entropy tests, send messages to different nodes and see
    //       what happens
  }

}
