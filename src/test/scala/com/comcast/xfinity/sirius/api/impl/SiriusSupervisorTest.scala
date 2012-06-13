package com.comcast.xfinity.sirius.api.impl

import membership._
import org.mockito.Mockito._
import com.typesafe.config.ConfigFactory
import com.comcast.xfinity.sirius.api.RequestHandler
import com.comcast.xfinity.sirius.writeaheadlog.LogWriter
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

object SiriusSupervisorTest {

  def createProbedTestSupervisor(admin: SiriusAdmin,
      handler: RequestHandler,
      logWriter: LogWriter,
      stateProbe: TestProbe,
      persistenceProbe: TestProbe,
      paxosProbe: TestProbe,
      membershipProbe: TestProbe,
      membershipAgent: Agent[MembershipMap])(implicit as: ActorSystem) = {
    TestActorRef(new SiriusSupervisor(admin, handler, logWriter, membershipAgent) {
      override def createStateActor(_handler: RequestHandler) = stateProbe.ref

      override def createPersistenceActor(_state: ActorRef, _writer: LogWriter) = persistenceProbe.ref

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

  var handler: RequestHandler = _
  var admin: SiriusAdmin = _
  var logWriter: LogWriter = _
  var siriusInfo: SiriusInfo = _

  var supervisor: TestActorRef[SiriusSupervisor] = _
  implicit val timeout: Timeout = (5 seconds)

  var expectedMap: MembershipMap = _

  before {
    actorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """))

    //setup mocks
    handler = mock[RequestHandler]
    admin = mock[SiriusAdmin]
    logWriter = mock[LogWriter]
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
        case Delete(_) => sender ! "Delete it".getBytes(); Some(this)
        case Put(_, _) => sender ! "Put it".getBytes(); Some(this)
      }
    })

    stateProbe = TestProbe()(actorSystem)
    stateProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Get(_) => sender ! "Got it".getBytes(); Some(this)
      }
    })

    persistenceProbe = TestProbe()(actorSystem)

    membershipAgent = mock[Agent[MembershipMap]]

    supervisor = SiriusSupervisorTest.createProbedTestSupervisor(
        admin, handler, logWriter, stateProbe, persistenceProbe, paxosProbe, membershipProbe, membershipAgent)(actorSystem)

    expectedMap = MembershipMap(siriusInfo -> MembershipData(membershipProbe.ref))
  }

  after {
    actorSystem.shutdown()
  }

  describe("a SiriusSupervisor") {
    it("should forward MembershipMessages to the membershipActor") {
      val membershipMessage: MembershipMessage = GetMembershipData
      supervisor ! membershipMessage
      membershipProbe.expectMsg(membershipMessage)
    }

    it("should forward GET messages to the stateActor") {
      var res = Await.result(supervisor ? (Get("1")), timeout.duration).asInstanceOf[Array[Byte]]
      assert(res != null)
      assert("Got it" === new String(res))
      stateProbe.expectMsg(Get("1"))
      noMoreMsgs()
    }
    it("should forward DELETE messages to the paxosActor") {
      var res = Await.result(supervisor ? (Delete("1")), timeout.duration).asInstanceOf[Array[Byte]]
      assert(res != null)
      assert("Delete it" === new String(res))
      paxosProbe.expectMsg(Delete("1"))
      noMoreMsgs()
    }
    it("should forward PUT messages to the paxosActor") {
      var msgBody = "some body".getBytes()
      var res = Await.result(supervisor ? (Put("1", msgBody)), timeout.duration).asInstanceOf[Array[Byte]]
      assert(res != null)
      assert("Put it" === new String(res))
      paxosProbe.expectMsg(Put("1", msgBody))
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