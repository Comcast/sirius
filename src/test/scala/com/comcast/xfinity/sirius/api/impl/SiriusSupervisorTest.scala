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

class SiriusSupervisorTest() extends NiceTest {

  var system: ActorSystem = _

  var paxosActor: TestProbe = _
  var persistenceActor: TestProbe = _
  var stateActor: TestProbe = _
  var membershipActor: TestProbe = _
  var nodeToJoin: TestProbe = _

  var handler: RequestHandler = _
  var admin: SiriusAdmin = _
  var logWriter: LogWriter = _
  var siriusInfo: SiriusInfo = _

  var supervisor: TestActorRef[SiriusSupervisor] = _
  implicit val timeout: Timeout = (5 seconds)

  var expectedMap: Map[SiriusInfo, MembershipData] = _

  before {
    system = spy(ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """)))

    //setup mocks
    handler = mock[RequestHandler]
    admin = mock[SiriusAdmin]
    logWriter = mock[LogWriter]
    siriusInfo = mock[SiriusInfo]

    //setup TestProbes
    nodeToJoin = TestProbe()(system)
    nodeToJoin.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Join(x) => sender ! x; Some(this)
      }
    })

    membershipActor = TestProbe()(system)
    membershipActor.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case AddMembers(x) => sender ! x; Some(this)
      }
    })

    paxosActor = TestProbe()(system)
    paxosActor.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Delete(_) => sender ! "Delete it".getBytes(); Some(this)
        case Put(_, _) => sender ! "Put it".getBytes(); Some(this)
      }
    })

    stateActor = TestProbe()(system)
    stateActor.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Get(_) => sender ! "Got it".getBytes(); Some(this)
      }
    })

    persistenceActor = TestProbe()(system)

    supervisor = TestActorRef(new SiriusSupervisor(admin, handler, logWriter))(system)
    supervisor.underlyingActor.paxosActor = paxosActor.ref
    supervisor.underlyingActor.persistenceActor = persistenceActor.ref
    supervisor.underlyingActor.stateActor = stateActor.ref
    supervisor.underlyingActor.membershipActor = membershipActor.ref

    expectedMap = Map[SiriusInfo, MembershipData](siriusInfo -> MembershipData(membershipActor.ref))
  }

  after {
    system.shutdown()

  }

  describe("a SiriusSupervisor") {
    describe("when receiving a JoinCluster message") {
      describe("the membershipActor TestProbe") {
        it("should not provide an empty membership data") {
          var membershipMap = Await.result(supervisor ? (JoinCluster(Some(nodeToJoin.ref), siriusInfo)), timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          assert(membershipMap.get(siriusInfo) != None)
        }
        it("should set the membership actor to itself") {
          val membershipMap = Await.result(supervisor ? (JoinCluster(Some(nodeToJoin.ref), siriusInfo)), timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          assert(membershipActor.ref === membershipMap.get(siriusInfo).get.membershipActor)
        }
        it("should send a message to start a new membership map with the result from the node to join") {
          Await.result(supervisor ? (JoinCluster(Some(nodeToJoin.ref), siriusInfo)), timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          membershipActor.expectMsg(AddMembers(expectedMap))
        }
      }
      describe("and is given a nodeToJoin") {
        it("should send a Join message to nodeToJoin's ActorRef") {
          Await.result(supervisor ? (JoinCluster(Some(nodeToJoin.ref), siriusInfo)), timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          nodeToJoin.expectMsg(Join(expectedMap))
        }
        it("should fail if nodeToJoin equals the current node")(pending)
      }
      describe("and is given no nodeToJoin") {
        it("should forward a NewMember message containing itself to the membershipActor"){
          Await.result(supervisor ? (JoinCluster(None, siriusInfo)), timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]]
          membershipActor.expectMsg(AddMembers(expectedMap))
        }
      }

    }
    it("should forward GET messages to the stateActor") {
      var res = Await.result(supervisor ? (Get("1")), timeout.duration).asInstanceOf[Array[Byte]]
      assert(res != null)
      assert("Got it" === new String(res))
      paxosActor.expectNoMsg(100 millis)
      persistenceActor.expectNoMsg(100 millis)
      stateActor.expectMsg(Get("1"))
    }
    it("should forward DELETE messages to the paxosActor") {
      var res = Await.result(supervisor ? (Delete("1")), timeout.duration).asInstanceOf[Array[Byte]]
      assert(res != null)
      assert("Delete it" === new String(res))
      paxosActor.expectMsg(Delete("1"))
      persistenceActor.expectNoMsg((100 millis) )
      stateActor.expectNoMsg((100 millis))
    }
    it("should forward PUT messages to the paxosActor") {
      var msgBody = "some body".getBytes()
      var res = Await.result(supervisor ? (Put("1", msgBody)), timeout.duration).asInstanceOf[Array[Byte]]
      assert(res != null)
      assert("Put it" === new String(res))
      paxosActor.expectMsg(Put("1", msgBody))
      persistenceActor.expectNoMsg((100 millis))
      stateActor.expectNoMsg((100 millis))
    }

  }

}