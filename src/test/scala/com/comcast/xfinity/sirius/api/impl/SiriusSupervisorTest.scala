package com.comcast.xfinity.sirius.api.impl

import membership._
import com.typesafe.config.ConfigFactory
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestProbe, TestActor, TestActorRef}
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.duration._
import akka.util.Timeout
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.SiriusResult
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}


@RunWith(classOf[JUnitRunner])
class SiriusSupervisorTest extends NiceTest with TimedTest {

  var actorSystem: ActorSystem = _

  var paxosProbe: TestProbe = _
  var persistenceProbe: TestProbe = _
  var stateProbe: TestProbe = _
  var membershipProbe: TestProbe = _

  var handler: RequestHandler = _
  var siriusLog: SiriusLog = _

  var supervisor: TestActorRef[SiriusSupervisor with SiriusSupervisor.DependencyProvider] = _

  implicit val timeout: Timeout = (5 seconds)

  before {
    actorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """))

    //setup mocks
    handler = mock[RequestHandler]
    siriusLog = mock[SiriusLog]

    membershipProbe = TestProbe()(actorSystem)
    membershipProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case msg: MembershipMessage => Some(this)
      }
    })

    paxosProbe = TestProbe()(actorSystem)
    paxosProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Delete(_) =>
          sender ! SiriusResult.some("Delete it")
          Some(this)
        case Put(_, _) =>
          sender ! SiriusResult.some("Put it")
          Some(this)
      }
    })

    stateProbe = TestProbe()(actorSystem)
    stateProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Get(_) =>
          sender ! SiriusResult.some("Got it")
          Some(this)
      }
    })

    persistenceProbe = TestProbe()(actorSystem)

    supervisor = TestActorRef(new SiriusSupervisor with SiriusSupervisor.DependencyProvider {
      val siriusStateAgent = Agent(new SiriusState)(context.system)
      val membershipAgent = Agent(Set[ActorRef]())(context.system)

      val stateSup: ActorRef = stateProbe.ref
      val membershipActor: ActorRef = membershipProbe.ref
      val orderingActor: ActorRef = paxosProbe.ref
      val statusSubsystem = TestProbe()(context.system).ref
      
    })(actorSystem)
  }

  after {
    actorSystem.shutdown()
  }

  def initializeSupervisor(supervisor: TestActorRef[SiriusSupervisor with SiriusSupervisor.DependencyProvider]) {
    val siriusStateAgent = supervisor.underlyingActor.siriusStateAgent
    siriusStateAgent send (SiriusState(false, true, true))
    // wait for agent to get updated, just in case
    assert(waitForTrue(siriusStateAgent().areSubsystemsInitialized, 1000, 250))

    val isInitializedFuture = supervisor ? SiriusSupervisor.IsInitializedRequest
    val expected = SiriusSupervisor.IsInitializedResponse(true)
    assert(expected === Await.result(isInitializedFuture, timeout.duration))
  }

  describe("a SiriusSupervisor") {
    it("should start in the uninitialized state") {
      val siriusState = supervisor.underlyingActor.siriusStateAgent()
      assert(false === siriusState.supervisorInitialized)
    }

    it("should transition into the initialized state") {
      initializeSupervisor(supervisor)
      val stateAgent = supervisor.underlyingActor.siriusStateAgent
      assert(true === stateAgent.await(timeout).supervisorInitialized)
    }

    it("should forward MembershipMessages to the membershipActor") {
      initializeSupervisor(supervisor)
      val membershipMessage: MembershipMessage = GetMembershipData
      supervisor ! membershipMessage
      membershipProbe.expectMsg(membershipMessage)
    }

    it("should forward GET messages to the stateActor") {
      initializeSupervisor(supervisor)
      val get = Get("1")
      val getAskFuture = supervisor ? get
      val expected = SiriusResult.some("Got it")
      assert(expected === Await.result(getAskFuture, timeout.duration))
      stateProbe.expectMsg(get)
      noMoreMsgs()
    }
    
    it("should forward DELETE messages to the paxosActor") {
      initializeSupervisor(supervisor)
      val delete = Delete("1")
      val deleteAskFuture = supervisor ? delete
      val expected = SiriusResult.some("Delete it")
      assert(expected === Await.result(deleteAskFuture, timeout.duration))
      paxosProbe.expectMsg(delete)
      noMoreMsgs()
    }

    it("should forward PUT messages to the paxosActor") {
      initializeSupervisor(supervisor)
      val put = Put("1", "someBody".getBytes)
      val putAskFuture = supervisor ? put
      val expected = SiriusResult.some("Put it")
      assert(expected === Await.result(putAskFuture, timeout.duration))
      paxosProbe.expectMsg(put)
      noMoreMsgs()
    }
  }

  def noMoreMsgs() {
    membershipProbe.expectNoMsg((100 millis))
    paxosProbe.expectNoMsg(100 millis)
    persistenceProbe.expectNoMsg((100 millis))
    stateProbe.expectNoMsg((100 millis))
  }

}
