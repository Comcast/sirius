package com.comcast.xfinity.sirius.api.impl

import membership.MembershipData
import org.mockito.Mockito.spy
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
import org.mockito.Matchers._
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.info.SiriusInfo
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SiriusImplTest extends NiceTest {

  var mockRequestHandler: RequestHandler = _


  var supervisorActorProbe: TestProbe = _
  var underTest: SiriusImpl = _
  var spiedAkkaSystem: ActorSystem = _
  val timeout: Timeout = (5 seconds)
  var logWriter: LogWriter = _
  var membershipMap: Map[SiriusInfo, MembershipData] = _


  before {
    spiedAkkaSystem = spy(ActorSystem("testsystem", ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    """)))

    membershipMap = mock[Map[SiriusInfo, MembershipData]];

    supervisorActorProbe = TestProbe()(spiedAkkaSystem)
    supervisorActorProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Get(_) => sender ! "Got it".getBytes(); Some(this)
        case Delete(_) => sender ! "Delete it".getBytes(); Some(this)
        case Put(_, _) => sender ! "Put it".getBytes(); Some(this)
        case JoinCluster(_, _) => Some(this)
        case GetMembershipData() => sender ! membershipMap; Some(this)
      }
    })

    logWriter = mock[LogWriter]
    doReturn(supervisorActorProbe.ref).when(spiedAkkaSystem).actorOf(any(classOf[Props]), anyString())

    underTest = new SiriusImpl(mockRequestHandler, spiedAkkaSystem, logWriter)


    supervisorActorProbe.expectMsg(JoinCluster(None, underTest.info))

  }

  after {
    spiedAkkaSystem.shutdown()
    spiedAkkaSystem.awaitTermination()
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
      supervisorActorProbe.expectMsg(GetMembershipData())
    }
  }
}
