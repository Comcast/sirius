package com.comcast.xfinity.sirius.api.impl

import membership._
import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestProbe, TestActorRef}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import org.scalatest.BeforeAndAfterAll


@RunWith(classOf[JUnitRunner])
class SiriusSupervisorTest extends NiceTest with BeforeAndAfterAll with TimedTest {

  implicit val actorSystem = ActorSystem("SiriusSupervisorTest")

  val paxosProbe = TestProbe()
  val persistenceProbe = TestProbe()
  val stateProbe = TestProbe()
  val membershipProbe = TestProbe()

  var supervisor: TestActorRef[SiriusSupervisor with SiriusSupervisor.DependencyProvider] = _

  override def afterAll() {
    actorSystem.shutdown()
  }

  before {
    supervisor = TestActorRef(new SiriusSupervisor with SiriusSupervisor.DependencyProvider {
      val siriusStateAgent: Agent[SiriusState] = Agent(new SiriusState)(context.system)
      val membershipAgent: Agent[Set[ActorRef]] = Agent(Set[ActorRef]())(context.system)

      val stateSup: ActorRef = stateProbe.ref
      val membershipActor: ActorRef = membershipProbe.ref
      val orderingActor: ActorRef = paxosProbe.ref
      val statusSubsystem: ActorRef = TestProbe().ref
      
    })
  }

  def initializeSupervisor(supervisor: TestActorRef[SiriusSupervisor with SiriusSupervisor.DependencyProvider]) {
    val siriusStateAgent = supervisor.underlyingActor.siriusStateAgent
    siriusStateAgent send (SiriusState(false, true))
    // wait for agent to get updated, just in case
    assert(waitForTrue(siriusStateAgent().areSubsystemsInitialized, 1000, 250))

    val senderProbe = TestProbe()
    senderProbe.send(supervisor, SiriusSupervisor.IsInitializedRequest)
    senderProbe.expectMsg(SiriusSupervisor.IsInitializedResponse(true))
  }

  describe("a SiriusSupervisor") {
    it("should start in the uninitialized state") {
      val siriusState = supervisor.underlyingActor.siriusStateAgent()
      assert(false === siriusState.supervisorInitialized)
    }

    it("should transition into the initialized state") {
      initializeSupervisor(supervisor)
      val stateAgent = supervisor.underlyingActor.siriusStateAgent
      waitForTrue(stateAgent().supervisorInitialized, 5000, 250)
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
      supervisor ! get
      stateProbe.expectMsg(get)
    }
    
    it("should forward DELETE messages to the paxosActor") {
      initializeSupervisor(supervisor)
      val delete = Delete("1")
      supervisor ! delete
      paxosProbe.expectMsg(delete)
    }

    it("should forward PUT messages to the paxosActor") {
      initializeSupervisor(supervisor)
      val put = Put("1", "someBody".getBytes)
      supervisor ! put
      paxosProbe.expectMsg(put)
    }
  }
}
