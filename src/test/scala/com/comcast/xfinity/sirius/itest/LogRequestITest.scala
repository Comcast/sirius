package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.{Helper, NiceTest}
import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.writeaheadlog.LogIteratorSource
import akka.util.duration._
import akka.actor.{ActorRef, Props, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.persistence._
import akka.testkit.{TestActorRef, TestProbe}
import com.comcast.xfinity.sirius.api.impl.persistence.RequestLogFromRemote
import com.comcast.xfinity.sirius.api.impl.persistence.InitiateTransfer
import com.comcast.xfinity.sirius.info.SiriusInfo
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.{membership, Put, OrderedEvent}
import membership._
import org.mockito.Mockito._


class LogRequestITest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("actorSystem")

  var remoteLogActor: TestActorRef[LogRequestActor] = _
  var siriusInfo: SiriusInfo = _
  var source: LogIteratorSource = _
  var logRequestWrapper: ActorRef = _
  var parentProbe: TestProbe = _
  var stateActorProbe: TestProbe = _
  var entries: List[OrderedEvent] = _
  var rawEntries: List[String] = _
  var membershipAgent: Agent[MembershipMap] = _

  var logRange: LogRange = _
  val chunkSize = 2
  val localSiriusId = "local:2552"
  val remoteSiriusId = "remote:2552"

  before {
    siriusInfo = mock[SiriusInfo]
    source = mock[LogIteratorSource]
    siriusInfo = mock[SiriusInfo]
    membershipAgent = mock[Agent[membership.MembershipMap]]
    parentProbe = TestProbe()(actorSystem)
    stateActorProbe = TestProbe()(actorSystem)
    membershipAgent = mock[Agent[MembershipMap]]
    entries = List(
      OrderedEvent(1, 300329, Put("key1", "A".getBytes)),
      OrderedEvent(2, 300329, Put("key1", "A".getBytes)),
      OrderedEvent(3, 300329, Put("key1", "A".getBytes))
    )

    logRange = new BoundedLogRange(0, 100)
    source = Helper.createMockSource(entries.iterator, logRange)

    logRequestWrapper = Helper.wrapActorWithMockedSupervisor(Props(createLogRequestActor()), parentProbe.ref, actorSystem)
    remoteLogActor = TestActorRef(createLogRequestActor())

    val remoteMembershipData = new MembershipData(remoteLogActor)
    when(membershipAgent.get()).thenReturn(Map(remoteSiriusId -> remoteMembershipData))
  }

  private def createLogRequestActor(): LogRequestActor = {
    new LogRequestActor(chunkSize, source, localSiriusId, stateActorProbe.ref, membershipAgent)
  }

  describe("a logRequestActor") {
    it("should start senders/receivers and receive TransferComplete when triggered by a RequestLogFromRemote message") {
      logRequestWrapper ! RequestLogFromRemote(remoteLogActor, logRange)
      parentProbe.expectMsg(5 seconds, TransferComplete)
    }

    it("should start senders/receivers and receive TransferComplete when triggered by a RequestLogFromAnyRemote message") {
      logRequestWrapper ! RequestLogFromAnyRemote(logRange)
      parentProbe.expectMsg(5 seconds, TransferComplete)
    }

    it("should initiate transfer of LogChunks by LogSender to LogReceiver") {
      remoteLogActor ! InitiateTransfer(parentProbe.ref, logRange)
      parentProbe.expectMsg(5 seconds, LogChunk(1, Vector(entries(0), entries(1))))
    }
  }

  override def afterAll() {
    actorSystem.shutdown()
  }
}
