package com.comcast.xfinity.sirius.api.impl.persistence

import com.comcast.xfinity.sirius.{Helper, NiceTest}
import akka.actor._
import akka.util.duration._
import org.scalatest.BeforeAndAfterAll
import akka.testkit.{TestProbe, TestActorRef}
import com.comcast.xfinity.sirius.writeaheadlog.LogIteratorSource
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.{OrderedEvent, Delete}
import org.mockito.Mockito._
import com.comcast.xfinity.sirius.api.impl.membership.MembershipHelper
import org.mockito.Matchers

class LogRequestActorTest extends NiceTest with BeforeAndAfterAll {
  implicit val actorSystem = ActorSystem("actorSystem")

  var remoteLogActor: TestActorRef[LogRequestActor] = _

  var parentProbe: TestProbe = _
  var logRequestWrapper: ActorRef = _
  var senderProbe: TestProbe = _
  var receiverProbe: TestProbe = _
  var persistenceActorProbe: TestProbe = _

  var source: LogIteratorSource = _
  var mockMembershipHelper: MembershipHelper = _
  var mockMembershipAgent: Agent[Set[ActorRef]] = _

  val chunkSize = 2
  val localSiriusRef: ActorRef = TestProbe().ref

  before {
    parentProbe = TestProbe()(actorSystem)
    senderProbe = TestProbe()(actorSystem)
    receiverProbe = TestProbe()(actorSystem)
    persistenceActorProbe = TestProbe()(actorSystem)

    mockMembershipHelper = mock[MembershipHelper]
    mockMembershipAgent = mock[Agent[Set[ActorRef]]]

    logRequestWrapper =
      Helper.wrapActorWithMockedSupervisor(Props(createLogRequestActor()), parentProbe.ref, actorSystem)
    remoteLogActor = TestActorRef(createLogRequestActor())

    source = Helper.createMockSource(
        OrderedEvent(1, 1, Delete("a")),
        OrderedEvent(2, 1, Delete("b")),
        OrderedEvent(3, 1, Delete("c")),
        OrderedEvent(4, 1, Delete("d")),
        OrderedEvent(6, 1, Delete("e")),
        OrderedEvent(7, 1, Delete("f")),
        OrderedEvent(8, 1, Delete("g"))
    )
  }

  private def createLogRequestActor(): LogRequestActor = {
    new LogRequestActor(chunkSize, source, localSiriusRef, mockMembershipAgent) {
      override lazy val membershipHelper = mockMembershipHelper
    }
  }

  describe("a LogRequestActor") {
    it("should report a 'no viable member to get logs from' message up to its parent as a failure") {
      doReturn(None).when(mockMembershipHelper).getRandomMember
      logRequestWrapper ! RequestLogFromAnyRemote(EntireLog, persistenceActorProbe.ref)
      parentProbe.expectMsg(5 seconds, TransferFailed(LogRequestActor.NO_MEMBER_FAIL_MSG))
    }

    it("should fire off a round of log requests when logs are requested from any remote.") {
      val probe = TestProbe()(actorSystem)
      val localLogRequestWrapper = Helper.wrapActorWithMockedSupervisor(
        Props(new LogRequestActor(chunkSize, source, localSiriusRef, mockMembershipAgent) {
        override def createReceiver(target: ActorRef): ActorRef = probe.ref
        override lazy val membershipHelper = mockMembershipHelper
      }), parentProbe.ref, actorSystem)

      doReturn(Some(probe.ref)).when(mockMembershipHelper).getRandomMember

      val logRange = new BoundedLogRange(0, 100)
      localLogRequestWrapper ! RequestLogFromAnyRemote(logRange, persistenceActorProbe.ref)
      probe.expectMsg(5 seconds, InitiateTransfer(probe.ref, logRange))
    }

    it("should create a LogSender and send it a Start message when InitiateTransfer is received") {
      val senderProbe = TestProbe()(actorSystem)
      val receiverProbe = TestProbe()(actorSystem)
      val localLogRequestWrapper =
        Helper.wrapActorWithMockedSupervisor(
          Props(new LogRequestActor(chunkSize, source, localSiriusRef, mockMembershipAgent) {
            override def createSender(): ActorRef = senderProbe.ref
        }),  parentProbe.ref, actorSystem)

      val logRange = new BoundedLogRange(0, 100)
      localLogRequestWrapper ! InitiateTransfer(receiverProbe.ref, logRange)
      senderProbe.expectMsg(1 seconds, Start(receiverProbe.ref, source, logRange, chunkSize))
    }
  }

  override def afterAll() {
    actorSystem.shutdown()
  }
}
