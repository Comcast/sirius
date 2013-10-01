package com.comcast.xfinity.sirius.api.impl
import akka.actor.{ActorContext, ActorRef, ActorSystem}
import akka.testkit.{TestProbe, TestActorRef}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import org.scalatest.BeforeAndAfterAll
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.api.impl.SiriusSupervisor.CheckPaxosMembership
import com.comcast.xfinity.sirius.uberstore.CompactionManager.{CompactionMessage, Compact}
import com.comcast.xfinity.sirius.api.impl.membership.MembershipActor.{GetMembershipData, MembershipMessage}
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import com.comcast.xfinity.sirius.api.impl.paxos.Replica

@RunWith(classOf[JUnitRunner])
class SiriusSupervisorTest extends NiceTest with BeforeAndAfterAll with TimedTest {

  def createSiriusSupervisor(stateAgent: Agent[SiriusState] = mock[Agent[SiriusState]],
                             membershipAgent: Agent[Set[ActorRef]] = mock[Agent[Set[ActorRef]]],
                             stateSup: ActorRef = TestProbe().ref,
                             membershipActor: ActorRef = TestProbe().ref,
                             stateBridge: ActorRef = TestProbe().ref,
                             statusSubsystem: ActorRef = TestProbe().ref,
                             paxosSupervisor: ActorRef = TestProbe().ref,
                             compactionManager: ActorRef = TestProbe().ref) = {
    val childProvider = new SiriusSupervisor.ChildProvider(null, null, null) {
      override def createStateAgent()(implicit context: ActorContext) = stateAgent
      override def createMembershipAgent()(implicit context: ActorContext) = membershipAgent
      override def createStateSupervisor(stateAgent: Agent[SiriusState])
                                        (implicit context: ActorContext) = stateSup
      override def createMembershipActor(membershipAgent: Agent[Set[ActorRef]])
                                        (implicit context: ActorContext) = membershipActor
      override def createStateBridge(stateSupervisor: ActorRef, siriusSupervisor: ActorRef, membershipHelper: MembershipHelper)
                                    (implicit context: ActorContext) = stateBridge
      override def createPaxosSupervisor(membershipAgent: Agent[Set[ActorRef]], performFun: Replica.PerformFun)
                                        (implicit context: ActorContext) = paxosSupervisor
      override def createStatusSubsystem(siriusSupervisor: ActorRef)(implicit context: ActorContext) = statusSubsystem
      override def createCompactionManager()(implicit context: ActorContext) = compactionManager
    }
    TestActorRef(new SiriusSupervisor(childProvider))
  }

  implicit val actorSystem = ActorSystem("SiriusSupervisorTest")

  val paxosProbe = TestProbe()
  val persistenceProbe = TestProbe()
  val stateProbe = TestProbe()
  val membershipProbe = TestProbe()
  val compactionProbe = TestProbe()
  var mockMembershipAgent: Agent[Set[ActorRef]] = mock[Agent[Set[ActorRef]]]
  var startStopProbe: TestProbe = _

  var supervisor: TestActorRef[SiriusSupervisor] = _

  override def afterAll() {
    actorSystem.shutdown()
  }

  before {
    startStopProbe = TestProbe()

    supervisor = createSiriusSupervisor(stateAgent = Agent(new SiriusState)(actorSystem),
                                        membershipAgent = mockMembershipAgent,
                                        stateSup = stateProbe.ref,
                                        membershipActor = membershipProbe.ref,
                                        paxosSupervisor = paxosProbe.ref,
                                        compactionManager = compactionProbe.ref)
  }

  def initializeSupervisor(supervisor: TestActorRef[SiriusSupervisor]) {
    val siriusStateAgent = supervisor.underlyingActor.siriusStateAgent
    siriusStateAgent send SiriusState(false, true)
    // wait for agent to get updated, just in case
    assert(waitForTrue(siriusStateAgent().areSubsystemsInitialized, 1000, 100))

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
      initializeOrdering(supervisor, Some(paxosProbe.ref))

      val membershipMessage: MembershipMessage = GetMembershipData
      supervisor ! membershipMessage
      membershipProbe.expectMsg(membershipMessage)
    }

    it("should forward GET messages to the stateActor") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      initializeOrdering(supervisor, Some(paxosProbe.ref))

      val get = Get("1")
      supervisor ! get
      stateProbe.expectMsg(get)
    }
    
    it("should forward DELETE messages to the paxosActor") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      initializeOrdering(supervisor, Some(paxosProbe.ref))

      val delete = Delete("1")
      supervisor ! delete
      paxosProbe.expectMsg(delete)
    }

    def initializeOrdering(supervisor: TestActorRef[SiriusSupervisor], expectedActor: Option[ActorRef]) {
      supervisor ! CheckPaxosMembership
      waitForTrue(supervisor.underlyingActor.orderingActor == expectedActor, 1000, 100)
    }

    it("should forward PUT messages to the paxosActor") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      initializeOrdering(supervisor, Some(paxosProbe.ref))

      val put = Put("1", "someBody".getBytes)
      supervisor ! put
      paxosProbe.expectMsg(put)
    }

    it("should forward compaction messages to the CompactionManager") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      val compactionMessage: CompactionMessage = Compact
      supervisor ! compactionMessage
      compactionProbe.expectMsg(compactionMessage)
    }

    it("should start the orderingActor if it checks membership and it's in there") {
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)

      supervisor ! CheckPaxosMembership

      assert(waitForTrue(supervisor.underlyingActor.orderingActor == Some(paxosProbe.ref), 1000, 100))
    }

    it("should stop the orderingActor if it checks membership and it's not in there") {
      // start up OrderingActor
      doReturn(Set(supervisor)).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      supervisor ! CheckPaxosMembership
      assert(waitForTrue(supervisor.underlyingActor.orderingActor == Some(paxosProbe.ref), 1000, 100))

      // so we can shut it down
      doReturn(Set()).when(mockMembershipAgent).get()
      initializeSupervisor(supervisor)
      supervisor ! CheckPaxosMembership
      assert(waitForTrue(supervisor.underlyingActor.orderingActor == None, 1000, 100))
    }
  }
}
