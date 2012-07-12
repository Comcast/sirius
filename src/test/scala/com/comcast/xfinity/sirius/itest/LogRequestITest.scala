package com.comcast.xfinity.sirius.itest

import com.comcast.xfinity.sirius.{TestHelper, NiceTest}
import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.writeaheadlog.{LogLinesSource, LogData, WriteAheadLogSerDe, LogDataSerDe}
import org.mockito.Mockito._
import akka.util.duration._
import akka.actor.{Actor, ActorRef, Props, ActorSystem}
import com.comcast.xfinity.sirius.api.impl.persistence._
import akka.testkit.{TestActorRef, TestProbe}
import com.comcast.xfinity.sirius.api.impl.persistence.RequestLogFromRemote
import com.comcast.xfinity.sirius.writeaheadlog.LogData
import scala.Some
import com.comcast.xfinity.sirius.api.impl.persistence.InitiateTransfer
import com.comcast.xfinity.sirius.api.impl.membership.{MembershipData, MemberInfo}


class LogRequestITest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("actorSystem")

  var remoteLogActor: TestActorRef[LogRequestActor] = _
  var source: LogLinesSource = _
  var logRequestWrapper: ActorRef = _
  var parentProbe: TestProbe = _
  var stateActorProbe: TestProbe = _
  var entries: List[LogData] = _
  var rawEntries: List[String] = _

  val chunkSize = 2


  before {
    source = mock[LogLinesSource]
    parentProbe = TestProbe()(actorSystem)
    stateActorProbe = TestProbe()(actorSystem)

    val serializer: LogDataSerDe = new  WriteAheadLogSerDe()
    entries = List[LogData](
      new LogData("PUT", "key1", 1L, 300392L, Some(Array[Byte](65, 124, 65))),
      new LogData("PUT", "key1", 2L, 300392L, Some(Array[Byte](65, 124, 65))),
      new LogData("PUT", "key1", 3L, 300392L, Some(Array[Byte](65, 124, 65)))
    )
    rawEntries = entries.map(serializer.serialize(_))

    source = TestHelper.createMockSource(rawEntries.iterator)

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
      parentProbe.expectMsg(5 seconds, LogChunk(1, Vector(rawEntries(0), rawEntries(1))))
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
