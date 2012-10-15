package com.comcast.xfinity.sirius.api.impl
import membership._
import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestProbe, TestActorRef}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import org.scalatest.BeforeAndAfterAll
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.api.impl.SiriusSupervisor.CheckPaxosMembership


@RunWith(classOf[JUnitRunner])
class SiriusSupervisorTest extends NiceTest with BeforeAndAfterAll with TimedTest {

  implicit val actorSystem = ActorSystem("SiriusSupervisorTest")

  val paxosProbe = TestProbe()
  val persistenceProbe = TestProbe()
  val stateProbe = TestProbe()
  val membershipProbe = TestProbe()
  val mockMembershipAgent: Agent[Set[ActorRef]] = mock[Agent[Set[ActorRef]]]
  var startStopProbe: TestProbe = _

  var supervisor: TestActorRef[SiriusSupervisor with SiriusSupervisor.DependencyProvider] = _

  override def afterAll() {
    actorSystem.shutdown()
  }

  before {
    startStopProbe = TestProbe()

    supervisor = TestActorRef(new SiriusSupervisor with SiriusSupervisor.DependencyProvider {
      val siriusStateAgent: Agent[SiriusState] = Agent(new SiriusState)(context.system)
      val membershipAgent: Agent[Set[ActorRef]] = mockMembershipAgent

      val stateSup: ActorRef = stateProbe.ref
      val membershipActor: ActorRef = membershipProbe.ref
      val stateBridge: ActorRef = TestProbe().ref
      var orderingActor: Option[ActorRef] = Some(paxosProbe.ref)
      val statusSubsystem: ActorRef = TestProbe().ref

      def ensureOrderingActorRunning() {
        orderingActor = Some(paxosProbe.ref)
        startStopProbe.ref ! 'Start
      }
      def ensureOrderingActorStopped() {
        orderingActor = None
        startStopProbe.ref ! 'Stop
      }
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
    doReturn(Set(supervisor)).when(mockMembershipAgent).get()
    it("should start in the uninitialized state") {
      val siriusState = supervisor.underlyingActor.siriusStateAgent()
      assert(false === siriusState.supervisorInitialized)
    }

    it("should transition into the initialized state") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      val stateAgent = supervisor.underlyingActor.siriusStateAgent
      waitForTrue(stateAgent().supervisorInitialized, 5000, 250)
    }

    it("should forward MembershipMessages to the membershipActor") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      val membershipMessage: MembershipMessage = GetMembershipData
      supervisor ! membershipMessage
      membershipProbe.expectMsg(membershipMessage)
    }

    it("should forward GET messages to the stateActor") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      val get = Get("1")
      supervisor ! get
      stateProbe.expectMsg(get)
    }
    
    it("should forward DELETE messages to the paxosActor") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      val delete = Delete("1")
      supervisor ! delete
      paxosProbe.expectMsg(delete)
    }

    it("should forward PUT messages to the paxosActor") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      val put = Put("1", "someBody".getBytes)
      supervisor ! put
      paxosProbe.expectMsg(put)
    }

    it("should fire off startOrderingActor if it checks membership and it's in there") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)

      supervisor ! CheckPaxosMembership

      startStopProbe.fishForMessage(hint="Expected 'Start evantually") {
        case 'Start => true
        case 'Stop => false
      }
    }

    it("should fire off stopOrderingActor if it checks membership and it's not in there") {
      doReturn(Set()).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)

      supervisor ! CheckPaxosMembership

      startStopProbe.expectMsg('Stop)
    }
  }
}
