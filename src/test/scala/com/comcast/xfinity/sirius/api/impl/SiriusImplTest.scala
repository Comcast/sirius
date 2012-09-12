package com.comcast.xfinity.sirius.api.impl

import akka.testkit.TestProbe
import akka.util.Timeout.durationToTimeout
import akka.util.duration.intToDurationInt
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import akka.testkit.TestActor
import akka.actor._
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent
import java.util.concurrent.TimeUnit
import scalax.file.Path
import com.comcast.xfinity.sirius.api.impl.membership._
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import com.comcast.xfinity.sirius.api.{SiriusConfiguration, RequestHandler, SiriusResult}

object SiriusImplTestCompanion {

  class ProbeWrapper(testProbe: TestProbe) extends Actor {
    def receive = {
      case any => testProbe.ref forward any
    }
  }

  // Create an extended impl for testing
  def createProbedSiriusImpl(handler: RequestHandler,
                             actorSystem: ActorSystem,
                             siriusLog: SiriusLog,
                             supProbe: TestProbe): SiriusImpl = {
    val createSiriusSupervisor: SiriusImpl.SiriusSupPropsFactory =
      (_handler: RequestHandler, _log: SiriusLog, _config: SiriusConfiguration)
      => Props(new ProbeWrapper(supProbe))
    new SiriusImpl(handler, siriusLog, new SiriusConfiguration, createSiriusSupervisor)(actorSystem)
  }
}


@RunWith(classOf[JUnitRunner])
class SiriusImplTest extends NiceTest with TimedTest {

  var mockRequestHandler: RequestHandler = _

  var supervisorActorProbe: TestProbe = _
  var underTest: SiriusImpl = _
  var actorSystem: ActorSystem = _
  val timeout: Timeout = (5 seconds)
  var siriusLog: SiriusLog = _
  var membership: Set[ActorRef] = _

  before {
    actorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
            akka.event-handlers = ["akka.testkit.TestEventListener"]
    """))

    membership = mock[Set[ActorRef]];

    supervisorActorProbe = TestProbe()(actorSystem)
    supervisorActorProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Get(_) =>
          sender ! SiriusResult.some("Got it")
          Some(this)
        case Delete(_) =>
          sender ! SiriusResult.some("Delete it")
          Some(this)
        case Put(_, _) =>
          sender ! SiriusResult.some("Put it")
          Some(this)
        case GetMembershipData =>
          sender ! membership
          Some(this)
        case CheckClusterConfig =>
          Some(this)
      }
    })

    siriusLog = mock[SiriusLog]

    underTest = SiriusImplTestCompanion
      .createProbedSiriusImpl(mockRequestHandler, actorSystem, siriusLog, supervisorActorProbe)

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

    describe(".isOnline") {
      // we need a better way of terminating...
      it ("returns false if the supervisor is shut down")(pending)

      it ("returns true if the supervisor is up and reports that it is initialized") {
        supervisorActorProbe.setAutoPilot(new TestActor.AutoPilot {
          def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
            case SiriusSupervisor.IsInitializedRequest =>
              sender ! SiriusSupervisor.IsInitializedResponse(true)
              None
          }
        })
        assert(true === underTest.isOnline)
      }

      it ("returns false if the supervisor is up and reports that it is not initialized") {
        supervisorActorProbe.setAutoPilot(new TestActor.AutoPilot {
          def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
            case SiriusSupervisor.IsInitializedRequest =>
              sender ! SiriusSupervisor.IsInitializedResponse(false)
              None
          }
        })
        assert(false === underTest.isOnline)
      }
    }

    describe(".shutdown") {
      it("must kill off the supervisor, not effecting the ActorSystem") {
        // XXX: it would be great to test that the agents are shutdown, but we don't have a
        //      good handle on those right now. I think for now it's ok to handwave over,
        //      since SiriusImpl's will generally be long lived and the Agent's shouldn't
        //      have much/any effect if they hang around.  The agent stuff will likely get
        //      pushed down anyway
        underTest.shutdown()
        assert(waitForTrue(underTest.supervisor.isTerminated, 2000, 200), "Supervisor should be terminated")
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
