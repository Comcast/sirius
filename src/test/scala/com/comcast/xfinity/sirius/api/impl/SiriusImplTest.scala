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
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import com.comcast.xfinity.sirius.info.SiriusInfo
import membership._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.SiriusResult

object SiriusImplTest {
  
  // Create an extended impl for testing
  def createProbedSiriusImpl(handler: RequestHandler, 
                             actorSystem: ActorSystem,
                             siriusLog: SiriusLog,
                             supProbe: TestProbe,
                             siriusStateAgent: Agent[SiriusState],
                             membershipAgent: Agent[MembershipMap]) = {
    new SiriusImpl(handler, actorSystem, siriusLog) {
      
      override def createSiriusSupervisor(_as: ActorSystem, 
          _handler: RequestHandler, 
          _info: SiriusInfo, 
          _log: SiriusLog,
          _siriusStateAgent: Agent[SiriusState],
          _membershipAgent: Agent[MembershipMap]) = supProbe.ref
          
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
  var siriusLog: SiriusLog = _
  var membershipMap: MembershipMap = _
  var siriusStateAgent: Agent[SiriusState] = _
  var membershipAgent: Agent[Map[SiriusInfo,MembershipData]] = _


  before {
    actorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
            akka.event-handlers = ["akka.testkit.TestEventListener"]
    """))

    membershipMap = mock[MembershipMap];

    supervisorActorProbe = TestProbe()(actorSystem)
    supervisorActorProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Get(_) =>
          sender ! SiriusResult.some("Got it".getBytes)
          Some(this)
        case Delete(_) =>
          sender ! SiriusResult.some("Delete it".getBytes)
          Some(this)
        case Put(_, _) => 
          sender ! SiriusResult.some("Put it".getBytes)
          Some(this)
        case JoinCluster(_, _) => 
          Some(this)
        case GetMembershipData =>
          sender ! membershipMap
          Some(this)
      }
    })

    siriusLog = mock[SiriusLog]

    underTest = SiriusImplTest.createProbedSiriusImpl(mockRequestHandler, 
        actorSystem, siriusLog, supervisorActorProbe, siriusStateAgent, membershipAgent)

  }

  after {
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  describe("a SiriusImpl") {
    it("should send a Get message to the supervisor actor when enqueueGet is called") {
      val key = "hello"
      val getFuture = underTest.enqueueGet(key)
      val expected = SiriusResult.some("Got it".getBytes)
      assert(expected === Await.result(getFuture, timeout.duration))
      supervisorActorProbe.expectMsg(Get(key))
    }

    it("should send a Put message to the supervisor actor when enqueuePut is called and get some \"ACK\" back") {
      val key = "hello"
      val body = "there".getBytes()
      val putFuture = underTest.enqueuePut(key, body)
      val expected = SiriusResult.some("Put it".getBytes)
      assert(expected === Await.result(putFuture, timeout.duration))
      supervisorActorProbe.expectMsg(Put(key, body))
    }

    it("should send a Delete message to the supervisor actor when enqueueDelete is called and get some \"ACK\" back") {
      val key = "hello"
      val deleteFuture = underTest.enqueueDelete(key)
      val expected = SiriusResult.some("Delete it".getBytes)
      assert(expected === Await.result(deleteFuture, timeout.duration))
      supervisorActorProbe.expectMsg(Delete(key))
    }

    it("should issue an \"ask\" GetMembership to the supervisor when getMembershipData is called") {
      val membershipFuture = underTest.getMembershipMap
      assert(membershipMap === Await.result(membershipFuture, timeout.duration))
      supervisorActorProbe.expectMsg(GetMembershipData)
    }

    it("should issue a JoinCluster message to the supervisor when joinCluster is called") {
      underTest.joinCluster(None)
      supervisorActorProbe.expectMsg(JoinCluster(None, underTest.info))

    }

  }
}
