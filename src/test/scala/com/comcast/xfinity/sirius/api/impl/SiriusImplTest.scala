package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.api.RequestHandler
import akka.dispatch.Await
import akka.testkit.TestProbe
import akka.util.Timeout.durationToTimeout
import akka.util.duration.intToDurationInt
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import akka.testkit.TestActor
import com.comcast.xfinity.sirius.NiceTest
import akka.actor._
import com.comcast.xfinity.sirius.writeaheadlog.LogWriter
import com.comcast.xfinity.sirius.info.SiriusInfo
import membership.{GetMembershipData, MembershipData, JoinCluster}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent

object SiriusImplTest {
  
  // Create an extended impl for testing
  def createProbedSiriusImpl(handler: RequestHandler, 
                             actorSystem: ActorSystem,
                             logWriter: LogWriter,
                             supProbe: TestProbe,
                             membershipAgent: Agent[Map[SiriusInfo, MembershipData]]) = {
    new SiriusImpl(handler, actorSystem, logWriter) {
      
      override def createSiriusSupervisor(_as: ActorSystem, 
          _handler: RequestHandler, 
          _info: SiriusInfo, 
          _writer: LogWriter, 
          _membershipAgent: Agent[Map[SiriusInfo, MembershipData]]) = supProbe.ref
          
    }
  }
}


@RunWith(classOf[JUnitRunner])
class SiriusImplTest extends NiceTest {

  var mockRequestHandler: RequestHandler = _

  var supervisorActorProbe: TestProbe = _
  var underTest: SiriusImpl = _
  var actorSystem: ActorSystem = _
  val timeout: Timeout = (5 seconds)
  var logWriter: LogWriter = _
  var membershipMap: Map[SiriusInfo, MembershipData] = _
  var membershipAgent: Agent[Map[SiriusInfo,MembershipData]] = _


  before {
    actorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
            akka.event-handlers = ["akka.testkit.TestEventListener"]
    """))

    membershipMap = mock[Map[SiriusInfo, MembershipData]];

    supervisorActorProbe = TestProbe()(actorSystem)
    supervisorActorProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Get(_) => sender ! "Got it".getBytes(); Some(this)
        case Delete(_) => sender ! "Delete it".getBytes(); Some(this)
        case Put(_, _) => sender ! "Put it".getBytes(); Some(this)
        case JoinCluster(_, _) => Some(this)
        case GetMembershipData => sender ! membershipMap; Some(this)
      }
    })

    logWriter = mock[LogWriter]

    underTest = SiriusImplTest.createProbedSiriusImpl(mockRequestHandler, actorSystem, logWriter, supervisorActorProbe, membershipAgent)

  }

  after {
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  describe("a SiriusImpl") {
    it("should send a Get message to the supervisor actor when enqueueGet is called") {
      val key = "hello"
      assert("Got it".getBytes() === Await.result(underTest.enqueueGet(key), timeout.duration).asInstanceOf[Array[Byte]])
      supervisorActorProbe.expectMsg(Get(key))
    }

    it("should send a Put message to the supervisor actor when enqueuePut is called and get some \"ACK\" back") {
      val key = "hello"
      val body = "there".getBytes()
      assert("Put it".getBytes() === Await.result(underTest.enqueuePut(key, body), timeout.duration).asInstanceOf[Array[Byte]])
      supervisorActorProbe.expectMsg(Put(key, body))
    }

    it("should send a Delete message to the supervisor actor when enqueueDelete is called and get some \"ACK\" back") {
      val key = "hello"
      assert("Delete it".getBytes() === Await.result(underTest.enqueueDelete(key), timeout.duration).asInstanceOf[Array[Byte]])
      supervisorActorProbe.expectMsg(Delete(key))
    }

    it("should issue an \"ask\" GetMembership to the supervisor when getMembershipData is called") {
      assert(membershipMap === Await.result(underTest.getMembershipMap, timeout.duration).asInstanceOf[Map[SiriusInfo, MembershipData]])
      supervisorActorProbe.expectMsg(GetMembershipData)
    }

    it("should issue a JoinCluster message to the supervisor when joinCluster is called") {
      underTest.joinCluster(None)
      supervisorActorProbe.expectMsg(JoinCluster(None, underTest.info))

    }

  }
}
