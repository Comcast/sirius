package com.comcast.xfinity.sirius.api.impl

import com.comcast.xfinity.sirius.api.RequestHandler
import akka.testkit.TestProbe
import akka.util.Timeout.durationToTimeout
import akka.util.duration.intToDurationInt
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import akka.testkit.TestActor
import com.comcast.xfinity.sirius.NiceTest
import akka.actor._
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.SiriusResult
import java.util.concurrent.TimeUnit
import scalax.file.Path
import com.comcast.xfinity.sirius.api.impl.membership._

object SiriusImplTest {
  
  // Create an extended impl for testing
  def createProbedSiriusImpl(handler: RequestHandler, 
                             actorSystem: ActorSystem,
                             siriusLog: SiriusLog,
                             supProbe: TestProbe,
                             siriusStateAgent: Agent[SiriusState],
                             membershipAgent: Agent[Set[ActorRef]],
                             clusterConfigPath: Path): SiriusImpl = {
    new SiriusImpl(handler, actorSystem, siriusLog, clusterConfigPath) {
      
      override def createSiriusSupervisor(_as: ActorSystem, 
          _handler: RequestHandler,
          _host: String,
          _port: Int,
          _log: SiriusLog,
          _siriusStateAgent: Agent[SiriusState],
          _membershipAgent: Agent[Set[ActorRef]],
          _clusterConfigPath: Path
          ) = supProbe.ref
          
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
  var membership: Set[ActorRef] = _
  var siriusStateAgent: Agent[SiriusState] = _
  var membershipAgent: Agent[Set[ActorRef]] = _
  var clusterConfigPath: Path = Path.fromString("")

  before {
    actorSystem = ActorSystem("testsystem", ConfigFactory.parseString("""
            akka.event-handlers = ["akka.testkit.TestEventListener"]
    """))

    membership = mock[Set[ActorRef]];

    supervisorActorProbe = TestProbe()(actorSystem)
    supervisorActorProbe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): Option[TestActor.AutoPilot] = msg match {
        case Get(_) =>
          sender ! SiriusResult.some("Got it")
          Some(this)
        case Delete(_) =>
          sender ! SiriusResult.some("Delete it")
          Some(this)
        case Put(_, _) => 
          sender ! SiriusResult.some("Put it")
          Some(this)
        case GetMembershipData =>
          sender ! membership
          Some(this)
        case CheckClusterConfig =>
          Some(this)
      }
    })

    siriusLog = mock[SiriusLog]

    underTest = SiriusImplTest.createProbedSiriusImpl(mockRequestHandler, 
        actorSystem, siriusLog, supervisorActorProbe, siriusStateAgent, membershipAgent, clusterConfigPath)

  }

  after {
    actorSystem.shutdown()
    actorSystem.awaitTermination()
  }

  describe("a SiriusImpl") {
    it("should send a Get message to the supervisor actor when enqueueGet is called") {
      val key = "hello"
      val getFuture = underTest.enqueueGet(key)
      val expected = SiriusResult.some("Got it")
      assert(expected === getFuture.get(1, TimeUnit.SECONDS))
      supervisorActorProbe.expectMsg(Get(key))
    }

    it("should send a Put message to the supervisor actor when enqueuePut is called and get some \"ACK\" back") {
      val key = "hello"
      val body = "there".getBytes()
      val putFuture = underTest.enqueuePut(key, body)
      val expected = SiriusResult.some("Put it")
      assert(expected === putFuture.get(1, TimeUnit.SECONDS))
      supervisorActorProbe.expectMsg(Put(key, body))
    }

    it("should send a Delete message to the supervisor actor when enqueueDelete is called and get some \"ACK\" back") {
      val key = "hello"
      val deleteFuture = underTest.enqueueDelete(key)
      val expected = SiriusResult.some("Delete it")
      assert(expected === deleteFuture.get(1, TimeUnit.SECONDS))
      supervisorActorProbe.expectMsg(Delete(key))
    }

    it("should issue an \"ask\" GetMembership to the supervisor when getMembershipData is called") {
      val membershipFuture = underTest.getMembership
      assert(membership === membershipFuture.get)
      supervisorActorProbe.expectMsg(GetMembershipData)
    }

    it("should issue a \"tell\" CheckClusterConfig when checkClusterConfig is called"){
      underTest.checkClusterConfig
      supervisorActorProbe.expectMsg(CheckClusterConfig)

    }

  }
}
