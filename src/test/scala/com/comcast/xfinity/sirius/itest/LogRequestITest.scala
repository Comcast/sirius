package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.{TestHelper, NiceTest}
import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.writeaheadlog.LogIteratorSource
import akka.util.duration._
import akka.actor.{ActorRef, Props, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.persistence._
import akka.testkit.{TestActorRef, TestProbe}
import com.comcast.xfinity.sirius.api.impl.persistence.RequestLogFromRemote
import scala.Some
import com.comcast.xfinity.sirius.api.impl.persistence.InitiateTransfer
import com.comcast.xfinity.sirius.api.impl.membership.{MembershipData, MemberInfo}
import com.comcast.xfinity.sirius.api.impl.{Put, OrderedEvent}


class LogRequestITest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("actorSystem")

  var remoteLogActor: TestActorRef[LogRequestActor] = _
  var source: LogIteratorSource = _
  var logRequestWrapper: ActorRef = _
  var parentProbe: TestProbe = _
  var stateActorProbe: TestProbe = _
  var entries: List[OrderedEvent] = _
  var rawEntries: List[String] = _

  val chunkSize = 2

  before {
    source = mock[LogIteratorSource]
    parentProbe = TestProbe()(actorSystem)
    stateActorProbe = TestProbe()(actorSystem)

    entries = List(
      OrderedEvent(1, 300329, Put("key1", "A".getBytes)),
      OrderedEvent(2, 300329, Put("key1", "A".getBytes)),
      OrderedEvent(3, 300329, Put("key1", "A".getBytes))
    )

    source = TestHelper.createMockSource(entries.iterator)

    logRequestWrapper = TestHelper.wrapActorWithMockedSupervisor(Props(new LogRequestActor(chunkSize, source, stateActorProbe.ref)), parentProbe.ref, actorSystem)
    remoteLogActor = TestActorRef(new LogRequestActor(chunkSize, source, stateActorProbe.ref))
  }

  describe("a logRequestActor") {
    it("should start senders/receivers and receive TransferComplete when triggered by a RequestLogFromRemote message") {
      logRequestWrapper ! RequestLogFromRemote(remoteLogActor)
      parentProbe.expectMsg(5 seconds, TransferComplete)
    }
    it("should initiate transfer of LogChunks by LogSender to LogReceiver") {
      remoteLogActor ! InitiateTransfer(parentProbe.ref)
      parentProbe.expectMsg(5 seconds, LogChunk(1, Vector(entries(0), entries(1))))
    }
    it("should use a member sent in a MemberInfo message to start and then complete a transfer") {
      logRequestWrapper ! MemberInfo(Some(MembershipData(remoteLogActor)))
      parentProbe.expectMsg(5 seconds, TransferComplete)
    }
  }

  override def afterAll() {
    actorSystem.shutdown()
  }
}
