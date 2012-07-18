package com.comcast.xfinity.sirius.api.impl

import membership._
import com.typesafe.config.ConfigFactory
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.admin.SiriusAdmin
import com.comcast.xfinity.sirius.NiceTest
import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestProbe, TestActor, TestActorRef}
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.duration._
import akka.util.Timeout
import com.comcast.xfinity.sirius.info.SiriusInfo
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.SiriusResult

object SiriusSupervisorTest {

  def createProbedTestSupervisor(admin: SiriusAdmin,
      handler: RequestHandler,
      siriusLog: SiriusLog,
      siriusInfo: SiriusInfo,
      stateProbe: TestProbe,
      persistenceProbe: TestProbe,
      paxosProbe: TestProbe,
      membershipProbe: TestProbe,
      siriusStateAgent: Agent[SiriusState],
      membershipAgent: Agent[MembershipMap])(implicit as: ActorSystem) = {
    TestActorRef(new SiriusSupervisor(admin, handler, siriusLog, siriusStateAgent, membershipAgent, siriusInfo) {

      override def createStateActor(_handler: RequestHandler) = stateProbe.ref

      override def createPersistenceActor(_state: ActorRef, _writer: SiriusLog) = persistenceProbe.ref

      override def createPaxosActor(_persistence: ActorRef) = paxosProbe.ref

      override def createMembershipActor(_membershipAgent: Agent[MembershipMap]) = membershipProbe.ref
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

  var membershipAgent: Agent[Map[SiriusInfo,MembershipData]] = _
  var siriusStateAgent: Agent[SiriusState] = _

  var handler: RequestHandler = _
  var admin: SiriusAdmin = _
  var siriusLog: SiriusLog = _
  var siriusInfo: SiriusInfo = _
  var siriusState: SiriusState = _

  var supervisor: TestActorRef[SiriusSupervisor {def createStateActor(_handler: RequestHandler): ActorRef; def createPersistenceActor(_state: ActorRef, _writer: SiriusLog): ActorRef; def createPaxosActor(_persistence: ActorRef): ActorRef; def createMembershipActor(_membershipAgent: Agent[_root_.com.comcast.xfinity.sirius.api.impl.membership.MembershipMap]): ActorRef}] = _
  implicit val timeout: Timeout = (5 seconds)

  before {
    actorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """))

    //setup mocks
    handler = mock[RequestHandler]
    admin = mock[SiriusAdmin]
    siriusLog = mock[SiriusLog]
    siriusInfo = mock[SiriusInfo]

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

    membershipAgent = mock[Agent[MembershipMap]]
    siriusState = new SiriusState
    siriusStateAgent = Agent(siriusState)(actorSystem)

    supervisor = SiriusSupervisorTest.createProbedTestSupervisor(
        admin, handler, siriusLog, siriusInfo, stateProbe, persistenceProbe, paxosProbe,
        membershipProbe, siriusStateAgent, membershipAgent)(actorSystem)
  }

  after {
    actorSystem.shutdown()
  }

  def initializeSupervisor(supervisor: ActorRef) {
    siriusState.updateStateActorState(SiriusState.StateActorState.Initialized)
    siriusState.updatePersistenceState(SiriusState.PersistenceState.Initialized)
    val isInitializedFuture = supervisor ? SiriusSupervisor.IsInitializedRequest
    val expected = SiriusSupervisor.IsInitializedResponse(true)
    assert(expected === Await.result(isInitializedFuture, timeout.duration))
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