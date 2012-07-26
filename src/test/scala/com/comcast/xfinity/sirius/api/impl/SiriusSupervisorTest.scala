package com.comcast.xfinity.sirius.api.impl

import membership._
import com.typesafe.config.ConfigFactory
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.NiceTest
import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestProbe, TestActor, TestActorRef}
import org.mockito.Mockito._
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.duration._
import akka.util.Timeout
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.SiriusResult
import scalax.file.Path
import scalax.io.Line.Terminators.NewLine
import scalax.io.LongTraversable

object SiriusSupervisorTestCompanion {

  def createProbedTestSupervisor(admin: SiriusAdmin,
      handler: RequestHandler,
      siriusLog: SiriusLog,
      siriusId: String,
      stateProbe: TestProbe,
      persistenceProbe: TestProbe,
      paxosProbe: TestProbe,
      membershipProbe: TestProbe,
      siriusStateAgent: Agent[SiriusState],
      membershipAgent: Agent[Set[ActorRef]],
      clusterConfigPath: Path)(implicit as: ActorSystem): TestActorRef[SiriusSupervisor] = {
    TestActorRef(new SiriusSupervisor(admin, handler, siriusLog, siriusStateAgent, membershipAgent, siriusId, clusterConfigPath) {

      override def createStateActor(_handler: RequestHandler) = stateProbe.ref

      override def createPersistenceActor(_state: ActorRef, _writer: SiriusLog) = persistenceProbe.ref

      override  def createPaxosActor(persistenceActor: ActorRef, agent: Agent[Set[ActorRef]]) = paxosProbe.ref

      override def createMembershipActor(_membershipAgent: Agent[Set[ActorRef]], _clusterConfigPath: Path) = membershipProbe.ref
    })
  }
}


@RunWith(classOf[JUnitRunner])
class SiriusSupervisorTest() extends NiceTest {

  var actorSystem: ActorSystem = _

  var paxosProbe: TestProbe = _
  var persistenceProbe: TestProbe = _
  var stateProbe: TestProbe = _
  var membershipProbe: TestProbe = _
  var nodeToJoinProbe: TestProbe = _

  var membershipAgent: Agent[Set[ActorRef]] = _
  var siriusStateAgent: Agent[SiriusState] = _

  var handler: RequestHandler = _
  var admin: SiriusAdmin = _
  var siriusLog: SiriusLog = _
  var siriusState: SiriusState = _
  var clusterConfigPath: Path = _


  var supervisor: TestActorRef[SiriusSupervisor] = _

  implicit val timeout: Timeout = (5 seconds)

  before {
    actorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """))

    //setup mocks
    handler = mock[RequestHandler]
    admin = mock[SiriusAdmin]
    siriusLog = mock[SiriusLog]
    clusterConfigPath = mock[Path]

    when(clusterConfigPath.lastModified).thenReturn(1L)
    when(clusterConfigPath.lines(NewLine, includeTerminator = false)).thenReturn(LongTraversable("dummyhost:8080"))

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

    membershipAgent = mock[Agent[Set[ActorRef]]]
    siriusState = new SiriusState
    siriusStateAgent = Agent(siriusState)(actorSystem)

    supervisor = SiriusSupervisorTestCompanion.createProbedTestSupervisor(
        admin, handler, siriusLog, "localhost:100", stateProbe, persistenceProbe, paxosProbe,
        membershipProbe, siriusStateAgent, membershipAgent, clusterConfigPath)(actorSystem)
  }

  after {
    actorSystem.shutdown()
  }

  def initializeSupervisor(supervisor: ActorRef) {
    siriusState.updateStateActorState(SiriusState.StateActorState.Initialized)
    siriusState.updatePersistenceState(SiriusState.PersistenceState.Initialized)
    siriusState.updateMembershipActorState(SiriusState.MembershipActorState.Initialized)
    val isInitializedFuture = supervisor ? SiriusSupervisor.IsInitializedRequest
    val expected = SiriusSupervisor.IsInitializedResponse(true)
    assert(expected === Await.result(isInitializedFuture, timeout.duration))
    membershipProbe.expectMsg(CheckClusterConfig)
  }

  describe("a SiriusSupervisor") {
    it("should start in the uninitialized state") {
      assert(siriusState.supervisorState === SiriusState.SupervisorState.Uninitialized)
    }

    it("should transition into the initialized state") {
      initializeSupervisor(supervisor)
      assert(siriusStateAgent.await(timeout).supervisorState === SiriusState.SupervisorState.Initialized)

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
