package com.comcast.xfinity.sirius.api.impl

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
import com.comcast.xfinity.sirius.api.impl.membership.AddMembers
import com.comcast.xfinity.sirius.api.impl.membership.MembershipData
import com.comcast.xfinity.sirius.api.impl.membership.Join
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

object SiriusSupervisorTest {
  
  def createProbedTestSupervisor(admin: SiriusAdmin, handler: RequestHandler, logWriter: LogWriter,
                             stateProbe: TestProbe, persistenceProbe: TestProbe,
                             paxosProbe: TestProbe, membershipProbe: TestProbe)(implicit as: ActorSystem) = {
    TestActorRef(new SiriusSupervisor(admin, handler, logWriter) {
      override def createStateActor(_handler: RequestHandler) = stateProbe.ref
      override def createPersistenceActor(_state: ActorRef, _writer: LogWriter) = persistenceProbe.ref
      override def createPaxosActor(_persistence: ActorRef) = paxosProbe.ref
      override def createMembershipActor() = membershipProbe.ref
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

  var handler: RequestHandler = _
  var admin: SiriusAdmin = _
  var logWriter: LogWriter = _
  var siriusInfo: SiriusInfo = _

  var supervisor: TestActorRef[SiriusSupervisor] = _
  implicit val timeout: Timeout = (5 seconds)

  var expectedMap: Map[SiriusInfo, MembershipData] = _

  before {
    actorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """))

    //setup mocks
    handler = mock[RequestHandler]
    admin = mock[SiriusAdmin]
    logWriter = mock[LogWriter]
    siriusInfo = mock[SiriusInfo]

    //setup TestProbes
    nodeToJoinProbe = TestProbe()(actorSystem)
    nodeToJoinProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Join(x) => sender ! x; Some(this)
      }
    })

    membershipProbe = TestProbe()(actorSystem)
    membershipProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case AddMembers(x) => sender ! x; Some(this)
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

    supervisor = SiriusSupervisorTest.createProbedTestSupervisor(admin, handler, logWriter, 
        stateProbe, persistenceProbe, paxosProbe, membershipProbe)(actorSystem)

    expectedMap = Map[SiriusInfo, MembershipData](siriusInfo -> MembershipData(membershipProbe.ref))
  }

  after {
    actorSystem.shutdown()
  }

  describe("a SiriusSupervisor") {
    describe("when receiving a JoinCluster message") {
      describe("the membershipActor TestProbe") {
        it("should not provide an empty membership data") {
          var membershipMap = Await.result(supervisor ? (JoinCluster(Some(nodeToJoinProbe.ref), siriusInfo)), timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          assert(membershipMap.get(siriusInfo) != None)
        }
        it("should set the membership actor to itself") {
          val membershipMap = Await.result(supervisor ? (JoinCluster(Some(nodeToJoinProbe.ref), siriusInfo)), timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          assert(membershipProbe.ref === membershipMap.get(siriusInfo).get.membershipActor)
        }
        it("should send a message to start a new membership map with the result from the node to join") {
          Await.result(supervisor ? (JoinCluster(Some(nodeToJoinProbe.ref), siriusInfo)), timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          membershipProbe.expectMsg(AddMembers(expectedMap))
        }
      }
      describe("and is given a nodeToJoin") {
        it("should send a Join message to nodeToJoin's ActorRef") {
          Await.result(supervisor ? (JoinCluster(Some(nodeToJoinProbe.ref), siriusInfo)), timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          nodeToJoinProbe.expectMsg(Join(expectedMap))
        }
      }
      describe("and is given no nodeToJoin") {
        it("should forward a NewMember message containing itself to the membershipActor") {
          Await.result(supervisor ? (JoinCluster(None, siriusInfo)), timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          membershipProbe.expectMsg(AddMembers(expectedMap))
        }
      }

    }
    it("should forward GET messages to the stateActor") {
      var res = Await.result(supervisor ? (Get("1")), timeout.duration).asInstanceOf[Array[Byte]]
      assert(res != null)
      assert("Got it" === new String(res))
      paxosProbe.expectNoMsg(100 millis)
      persistenceProbe.expectNoMsg(100 millis)
      stateProbe.expectMsg(Get("1"))
    }
    it("should forward DELETE messages to the paxosActor") {
      var res = Await.result(supervisor ? (Delete("1")), timeout.duration).asInstanceOf[Array[Byte]]
      assert(res != null)
      assert("Delete it" === new String(res))
      paxosProbe.expectMsg(Delete("1"))
      persistenceProbe.expectNoMsg((100 millis))
      stateProbe.expectNoMsg((100 millis))
    }
    it("should forward PUT messages to the paxosActor") {
      var msgBody = "some body".getBytes()
      var res = Await.result(supervisor ? (Put("1", msgBody)), timeout.duration).asInstanceOf[Array[Byte]]
      assert(res != null)
      assert("Put it" === new String(res))
      paxosProbe.expectMsg(Put("1", msgBody))
      persistenceProbe.expectNoMsg((100 millis))
      stateProbe.expectNoMsg((100 millis))
    }

  }

}