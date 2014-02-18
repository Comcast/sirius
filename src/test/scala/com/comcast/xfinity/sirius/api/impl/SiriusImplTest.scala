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
package com.comcast.xfinity.sirius.api.impl

import akka.testkit.TestProbe
import akka.util.Timeout.durationToTimeout
import scala.concurrent.duration._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import akka.testkit.TestActor
import akka.actor._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.util.concurrent.TimeUnit
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor._
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import com.comcast.xfinity.sirius.api.{SiriusConfiguration, SiriusResult}
import status.NodeStats.FullNodeStatus
import status.StatusWorker._

object SiriusImplTestCompanion {

  object ProbeWrapper{
    def props(testProbe: TestProbe): Props = {
      Props(classOf[ProbeWrapper], testProbe)
    }
  }
  class ProbeWrapper(testProbe: TestProbe) extends Actor {
    def receive = {
      case any => testProbe.ref forward any
    }
  }

  // Create an extended impl for testing
  def createProbedSiriusImpl(actorSystem: ActorSystem,
                             supProbe: TestProbe): SiriusImpl = {
    new SiriusImpl(new SiriusConfiguration, ProbeWrapper.props(supProbe))(actorSystem)
  }
}


@RunWith(classOf[JUnitRunner])
class SiriusImplTest extends NiceTest with TimedTest {

  var supervisorActorProbe: TestProbe = _
  var underTest: SiriusImpl = _
  implicit var actorSystem: ActorSystem = _
  val timeout: Timeout = 5 seconds
  var membership: Map[String, Option[ActorRef]] = _
  var actorContext: ActorContext = _

  val mockNodeStatus = mock[FullNodeStatus]

  before {
    actorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
            akka.loggers = ["akka.testkit.TestEventListener"]
    """))

    membership = mock[Map[String, Option[ActorRef]]]

    supervisorActorProbe = TestProbe()(actorSystem)
    supervisorActorProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
        case Get(_) =>
          sender ! SiriusResult.some("Got it")
          this
        case Delete(_) =>
          sender ! SiriusResult.some("Delete it")
          this
        case Put(_, _) =>
          sender ! SiriusResult.some("Put it")
          this
        case GetMembershipData =>
          sender ! membership
          this
        case CheckClusterConfig =>
          this
        case GetStatus =>
          sender ! mockNodeStatus
          this
      }
    })

    underTest = SiriusImplTestCompanion
      .createProbedSiriusImpl(actorSystem, supervisorActorProbe)

  }

  after {
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  describe("a SiriusImpl") {
    it("should send a Get message to the supervisor actor when enqueueGet is called") {
      val key = "hello"
      val getFuture = underTest.enqueueGet(key)
      val expected = SiriusResult.some("Got it")
      assert(expected === getFuture.get(1, TimeUnit.SECONDS))
      supervisorActorProbe.expectMsg(Get(key))
    }

    it("should send a Put message to the supervisor actor when enqueuePut is called and get some \"ACK\" back") {
      val key = "hello"
      val body = "there".getBytes
      val putFuture = underTest.enqueuePut(key, body)
      val expected = SiriusResult.some("Put it")
      assert(expected === putFuture.get(1, TimeUnit.SECONDS))
      supervisorActorProbe.expectMsg(Put(key, body))
    }

    it("should send a Delete message to the supervisor actor when enqueueDelete is called and get some \"ACK\" back") {
      val key = "hello"
      val deleteFuture = underTest.enqueueDelete(key)
      val expected = SiriusResult.some("Delete it")
      assert(expected === deleteFuture.get(1, TimeUnit.SECONDS))
      supervisorActorProbe.expectMsg(Delete(key))
    }

    it("should issue an \"ask\" GetMembership to the supervisor when getMembershipData is called") {
      val membershipFuture = underTest.getMembership
      assert(membership === membershipFuture.get)
      supervisorActorProbe.expectMsg(GetMembershipData)
    }

    it("should issue a \"tell\" CheckClusterConfig when checkClusterConfig is called") {
      underTest.checkClusterConfig()
      supervisorActorProbe.expectMsg(CheckClusterConfig)
    }

    it ("must send a GetStatus message to the supervisor and return the resultant future") {
      val nodeStatusFuture = underTest.getStatus
      assert(mockNodeStatus === nodeStatusFuture.get(1, TimeUnit.SECONDS))
      supervisorActorProbe.expectMsg(GetStatus)
    }

    describe(".isOnline") {
      // we need a better way of terminating...
      it ("returns false if the supervisor is shut down")(pending)

      it ("returns true if the supervisor is up and reports that it is initialized") {
        supervisorActorProbe.setAutoPilot(new TestActor.AutoPilot {
          def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
            msg match {
              case SiriusSupervisor.IsInitializedRequest =>
                sender ! SiriusSupervisor.IsInitializedResponse(initialized = true)
                TestActor.NoAutoPilot
            }
        })
        assert(true === underTest.isOnline)
      }

      it ("returns false if the supervisor is up and reports that it is not initialized") {
        supervisorActorProbe.setAutoPilot(new TestActor.AutoPilot {
          def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
            case SiriusSupervisor.IsInitializedRequest =>
              sender ! SiriusSupervisor.IsInitializedResponse(initialized = false)
              TestActor.NoAutoPilot
          }
        })
        assert(false === underTest.isOnline)
      }
    }

    describe(".shutdown") {
      it("must kill off the supervisor, not affecting the ActorSystem") {
        // XXX: it would be great to test that the agents are shutdown, but we don't have a
        //      good handle on those right now. I think for now it's ok to handwave over,
        //      since SiriusImpl's will generally be long lived and the Agent's shouldn't
        //      have much/any effect if they hang around.  The agent stuff will likely get
        //      pushed down anyway
        val terminationProbe = TestProbe()
        terminationProbe.watch(underTest.supervisor)
        underTest.shutdown()
        terminationProbe.expectMsgClass(classOf[Terminated])
        assert(!underTest.actorSystem.isTerminated, "ActorSystem should not be terminated")
        assert(false === underTest.isOnline)
      }

      it("should execute shutdownOperations when shutdown is called and only does so once") {
        var timesShutdownWasCalled = 0

        underTest.onShutdown({
          timesShutdownWasCalled += 1
        })
        underTest.shutdown()
        assert(1 === timesShutdownWasCalled)

      }
    }

  }
}
