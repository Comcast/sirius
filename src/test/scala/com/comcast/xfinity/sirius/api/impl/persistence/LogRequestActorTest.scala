package com.comcast.xfinity.sirius.api.impl.persistence

import com.comcast.xfinity.sirius.NiceTest
import akka.actor._
import org.mockito.Mockito._
import akka.util.duration._
import org.scalatest.BeforeAndAfterAll
import akka.testkit.{TestProbe, TestActorRef}
import com.comcast.xfinity.sirius.writeaheadlog.LogLinesSource
import com.comcast.xfinity.sirius.api.impl.membership.{MemberInfo, GetRandomMember, MembershipData}

class LogRequestActorTest extends NiceTest with BeforeAndAfterAll {
  implicit val actorSystem = ActorSystem("actorSystem")

  var remoteLogActor: TestActorRef[LogRequestActor] = _
  var parentProbe: TestProbe = _
  var source: LogLinesSource = _
  var logRequestWrapper: ActorRef = _
  var senderProbe: TestProbe = _
  var receiverProbe: TestProbe = _

  val chunkSize = 2

  def wrapActorWithMockedSupervisor(inner: Props, parent: ActorRef): ActorRef = {
    actorSystem.actorOf(Props(new Actor {
      val innerRef = context.actorOf(inner)
      def receive = {
        case x => if (sender == innerRef) {
          parent forward x
        } else {
          innerRef forward x
        }
      }
    }))
  }

  before {
    source = mock[LogLinesSource]
    parentProbe = TestProbe()(actorSystem)

    senderProbe = TestProbe()(actorSystem)
    receiverProbe = TestProbe()(actorSystem)

    logRequestWrapper = wrapActorWithMockedSupervisor(Props(new LogRequestActor(chunkSize, source)), parentProbe.ref)
    remoteLogActor = TestActorRef(new LogRequestActor(chunkSize, source))

    when(source.createLinesIterator()).thenReturn(Iterator("a", "b", "c", "d", "e", "f", "g"))
  }

  describe("a LogRequestActor") {
    it("should report a 'no viable member to get logs from' message up to its parent as a failure") {
      logRequestWrapper ! MemberInfo(None)
      parentProbe.expectMsg(5 seconds, TransferFailed(LogRequestActor.NO_MEMBER_FAIL_MSG))
    }
    it("should use a member sent in a MemberInfo message to fire off a round of log request") {
      val probe = TestProbe()(actorSystem)
      val localLogRequestWrapper = wrapActorWithMockedSupervisor(Props(new LogRequestActor(chunkSize, source) {
        override def createReceiver(): ActorRef = probe.ref
      }), parentProbe.ref)

      localLogRequestWrapper ! MemberInfo(Some(MembershipData(probe.ref)))
      probe.expectMsg(5 seconds, InitiateTransfer(probe.ref))
    }
    it("should create a LogSender and send it a Start message when InitiateTransfer is received") {
      val senderProbe = TestProbe()(actorSystem)
      val receiverProbe = TestProbe()(actorSystem)
      val localLogRequestWrapper =
        wrapActorWithMockedSupervisor(Props(new LogRequestActor(chunkSize, source) {
          override def createSender(): ActorRef = senderProbe.ref
        }),  parentProbe.ref)

      localLogRequestWrapper ! InitiateTransfer(receiverProbe.ref)
      senderProbe.expectMsg(1 seconds, Start(receiverProbe.ref, source, chunkSize))
    }
    it("should ask its parent to go find a random member when RequestLogFromRemote is sent with no remote ref") {
      logRequestWrapper ! RequestLogFromRemote
      parentProbe.expectMsg(5 seconds, GetRandomMember)
    }
    it("should start senders/receivers and receive TransferComplete when triggered by a RequestLogFromRemote message") {
      // TODO this is really an itest
      logRequestWrapper ! RequestLogFromRemote(remoteLogActor)
      parentProbe.expectMsg(5 seconds, TransferComplete)
    }
    it("should initiate transfer of LogChunks by LogSender to LogReceiver") {
      // TODO this is really an itest
      remoteLogActor ! InitiateTransfer(parentProbe.ref)
      parentProbe.expectMsg(5 seconds, LogChunk(1, Vector("a", "b")))
    }
    it("should use a member sent in a MemberInfo message to start and then complete a transfer") {
      // TODO this is really an itest
      logRequestWrapper ! MemberInfo(Some(MembershipData(remoteLogActor)))
      parentProbe.expectMsg(5 seconds, TransferComplete)
    }
  }

  override def afterAll() {
    actorSystem.shutdown()
  }
}
