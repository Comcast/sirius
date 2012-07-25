package com.comcast.xfinity.sirius.api.impl.paxos

import com.comcast.xfinity.sirius.NiceTest
import akka.agent.Agent
import akka.actor.{Props, ActorRef, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.{Delete, Put, NonCommutativeSiriusRequest}
import com.comcast.xfinity.sirius.api.impl.paxos.PaxosMessages.RequestPerformed
import akka.util.duration._
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import akka.testkit.{TestLatch, TestProbe}
import akka.dispatch.Await


object PaxosITest {
  class TestNode(membership: Agent[Set[ActorRef]], decisionLatch: TestLatch)(implicit as: ActorSystem) {
    var decisions = Set[(Long, NonCommutativeSiriusRequest)]()

    val paxosSup = as.actorOf(Props(
      PaxosSup(membership, {
        case decision if decisions.contains(decision) =>
          false
        case decision =>
          decisions += decision
          decisionLatch.countDown()
          true
      })
    ))

    def hasDecisionFor(req: NonCommutativeSiriusRequest): Boolean =
      decisions.exists(req == _._2)
  }
}

class PaxosITest extends NiceTest with BeforeAndAfterAll {

  import PaxosITest._

  val config = ConfigFactory.parseString("""
      akka {
        loglevel=DEBUG
      }
    """)

  implicit val as = ActorSystem("PaxosITest", ConfigFactory.load(config))

  override def afterAll {
    as.shutdown()
  }

  describe("The Paxos subsystem") {
    it ("must arrive at a decision when all requests are sent to a single node, and " +
        "the initiators must be properly notified") {
      val membership = Agent(Set[ActorRef]())

      // 3 nodes x 3 requests = 9 applied decisions
      val decisionLatch = TestLatch(9)

      // Each of these TestNodes contains a paxos system and
      // set of performed decisions.  The set of decisions is
      // updated and the decisionLatch is counted down when
      // a new decision arrives
      val node1 = new TestNode(membership, decisionLatch)
      val node2 = new TestNode(membership, decisionLatch)
      val node3 = new TestNode(membership, decisionLatch)

      // with the nodes created, establish membership
      membership send (_ + node1.paxosSup)
      membership send (_ + node2.paxosSup)
      membership send (_ + node3.paxosSup)

      // stage and send requests
      val req1 = Delete("A")
      val req2 = Put("B", "C".getBytes)
      val req3 = Delete("D")

      // each request will want an explicit sender
      // to receive the RequestPerformed message
      val requester1 = TestProbe()
      val requester2 = TestProbe()
      val requester3 = TestProbe()

      requester1.send(node1.paxosSup, PaxosSup.Submit(req1))
      requester2.send(node1.paxosSup, PaxosSup.Submit(req2))
      requester3.send(node1.paxosSup, PaxosSup.Submit(req3))

      // Wait for the each decision to complete
      // Give these extra time, the first scout round
      // generally times out since the membership is
      // empty on initialization
      requester1.expectMsg(7 seconds, RequestPerformed)
      requester2.expectMsg(7 seconds, RequestPerformed)
      requester3.expectMsg(7 seconds, RequestPerformed)

      // Wait for the decision to actually be received
      // on each node, the request RequestPerformed
      // message only indicates that one node has
      // received and processed the decision
      Await.ready(decisionLatch, 3 seconds)

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