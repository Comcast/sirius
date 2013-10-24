package com.comcast.xfinity.sirius.uberstore

import com.comcast.xfinity.sirius.NiceTest
import org.scalatest.BeforeAndAfterAll
import akka.testkit.{TestProbe, TestActorRef}
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import org.mockito.Mockito._
import akka.actor.{Terminated, ActorSystem}
import com.comcast.xfinity.sirius.uberstore.CompactionActor.{CompactionFailed, CompactionComplete}
import com.comcast.xfinity.sirius.uberstore.CompactionManager.Compact

class CompactionActorTest extends NiceTest with BeforeAndAfterAll {

  implicit val actorSystem = ActorSystem("CompactionTest")

  def makeMockCompactionActor(siriusLog: SiriusLog = mock[SiriusLog]) = TestActorRef(
    new CompactionActor(siriusLog)
  )

  override def afterAll() {
    actorSystem.shutdown()
  }

  describe ("A compaction actor") {
    it ("should die and notify CompactionFailed if the underlying compaction errors out") {
      val terminationProbe = TestProbe()
      val mockSiriusLog = mock[SiriusLog]
      val senderProbe = TestProbe()
      val underTest = makeMockCompactionActor(mockSiriusLog)
      val exception = new IllegalStateException("ERROR")
      doThrow(exception).when(mockSiriusLog).compact()
      terminationProbe.watch(underTest)

      senderProbe.send(underTest, Compact)
      verify(mockSiriusLog).compact()

      senderProbe.expectMsg(CompactionFailed(exception))
      terminationProbe.expectMsg(Terminated(underTest))
    }

    it ("should call through to the siriusLog's compact method") {
      val mockSiriusLog = mock[SiriusLog]
      val underTest = makeMockCompactionActor(mockSiriusLog)

      underTest ! Compact
      verify(mockSiriusLog).compact()
    }

    it ("should report CompactionComplete and stop itself if the compact method returns with no errors") {
      val terminationProbe = TestProbe()
      val mockSiriusLog = mock[SiriusLog]
      val underTest = makeMockCompactionActor(mockSiriusLog)
      val senderProbe = TestProbe()
      terminationProbe.watch(underTest)

      senderProbe.send(underTest, Compact)

      senderProbe.expectMsg(CompactionComplete)
      terminationProbe.expectMsg(Terminated(underTest))
    }
  }
}
