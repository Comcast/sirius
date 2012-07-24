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

class PaxosITest extends NiceTest with BeforeAndAfterAll {

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

      // each node has an associated set which it updates
      // when it receives a deicion. Also, upon receiving
      // a new decision it updates the countdown latch
      // this can probably be refactored out to be pretty,
      // but for the time being it works
      var node1Decisions = Set[(Long, NonCommutativeSiriusRequest)]()
      val node1 = as.actorOf(Props(
        PaxosSup(membership, {
          case decision if node1Decisions.contains(decision) =>
            false
          case decision =>
            node1Decisions += decision
            decisionLatch.countDown()
            true
        })
      ))

      var node2Decisions = Set[(Long, NonCommutativeSiriusRequest)]()
      val node2 = as.actorOf(Props(
        PaxosSup(membership, {
          case decision if node2Decisions.contains(decision) =>
            false
          case decision =>
            node2Decisions += decision
            decisionLatch.countDown()
            true
        })
      ))

      var node3Decisions = Set[(Long, NonCommutativeSiriusRequest)]()
      val node3 = as.actorOf(Props(
        PaxosSup(membership, {
          case decision if node3Decisions.contains(decision) =>
            false
          case decision =>
            node3Decisions += decision
            decisionLatch.countDown()
            true
        })
      ))

      // with the nodes created, establish membership
      membership send (_ + node1)
      membership send (_ + node2)
      membership send (_ + node3)

      // stage and send requests
      val req1 = Delete("A")
      val req2 = Put("B", "C".getBytes)
      val req3 = Delete("D")

      // each request will want an explicit sender
      // to receive the RequestPerformed message
      val requester1 = TestProbe()
      val requester2 = TestProbe()
      val requester3 = TestProbe()

      requester1.send(node1, PaxosSup.Submit(req1))
      requester2.send(node1, PaxosSup.Submit(req2))
      requester3.send(node1, PaxosSup.Submit(req3))

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

      // Associative property y'all
      assert(node1Decisions === node2Decisions,
        "Node 1 & Node 2's decisions did not match")
      assert(node2Decisions === node3Decisions,
        "Node 2 & Node 3's decisions did not match")

      List(node1Decisions, node2Decisions, node3Decisions).foreach(
        (decisions) => {
          assert(decisions.exists((decision) => req1 == decision._2),
            req1 + " was missing from " + decisions)
          assert(decisions.exists((decision) => req2 == decision._2),
            req2 + " was missing from " + decisions)
          assert(decisions.exists((decision) => req3 == decision._2),
            req3 + " was missing from " + decisions)
        }
      )

      // TODO: throw some more in here?
    }

    // TODO: some entropy tests, send messages to different nodes and see
    //       what happens
  }

}