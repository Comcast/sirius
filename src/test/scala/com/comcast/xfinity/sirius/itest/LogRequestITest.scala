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
import akka.agent.Agent
import com.comcast.xfinity.sirius.api.impl.{membership, Put, OrderedEvent}
import org.mockito.Mockito._


class LogRequestITest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("actorSystem")

  var remoteLogActor: TestActorRef[LogRequestActor] = _
  var paxosProbe : TestProbe = _
  var source: LogIteratorSource = _
  var logRequestWrapper: ActorRef = _
  var parentProbe: TestProbe = _
  var stateActorProbe: TestProbe = _
  var entries: List[OrderedEvent] = _
  var rawEntries: List[String] = _
  var membershipAgent: Agent[Set[ActorRef]] = _

  var logRange: LogRange = _
  val chunkSize = 2
  val localSiriusRef = TestProbe().ref

  before {
    source = mock[LogIteratorSource]
    membershipAgent = mock[Agent[Set[ActorRef]]]
    parentProbe = TestProbe()
    stateActorProbe = TestProbe()
    entries = List(
      OrderedEvent(1, 300329, Put("key1", "A".getBytes)),
      OrderedEvent(2, 300329, Put("key1", "A".getBytes)),
      OrderedEvent(3, 300329, Put("key1", "A".getBytes))
    )

    logRange = new BoundedLogRange(0, 100)
    source = Helper.createMockSource(entries.iterator, logRange)

    logRequestWrapper = Helper.wrapActorWithMockedSupervisor(Props(createLogRequestActor()), parentProbe.ref, actorSystem)
    remoteLogActor = TestActorRef(createLogRequestActor())
    paxosProbe = TestProbe()(actorSystem)

    when(membershipAgent()).thenReturn(Set[ActorRef](remoteLogActor, localSiriusRef))
  }

  private def createLogRequestActor(): LogRequestActor = {
    new LogRequestActor(chunkSize, source, localSiriusRef, stateActorProbe.ref, membershipAgent)
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
