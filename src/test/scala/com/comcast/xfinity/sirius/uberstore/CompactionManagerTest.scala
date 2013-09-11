package com.comcast.xfinity.sirius.uberstore

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.NiceTest
import akka.actor.{PoisonPill, ActorRef, ActorSystem}
import akka.util.duration._
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import akka.testkit.{TestProbe, TestActorRef}
import javax.management.{ObjectName, MBeanServer}
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.uberstore.CompactionActor.CompactionComplete
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.comcast.xfinity.sirius.uberstore.CompactionManager.{CompactionManagerInfoMBean, GetState, Compact}
import akka.util.Timeout

class CompactionManagerTest extends NiceTest with BeforeAndAfterAll {

  implicit val timeout: Timeout = 5.seconds
  implicit var actorSystem: ActorSystem = ActorSystem("CompactionSchedulingTest")
  var compactionsStarted: Int = _

  def makeMockCompactionManager(siriusLog: SiriusLog = mock[SiriusLog],
                                        testCompactionActor: ActorRef = TestProbe().ref,
                                        testCurrentCompactionActor: Option[ActorRef] = None,
                                        testLastStarted: Option[Long] = None,
                                        testLastDuration: Option[Long] = None,
                                        testMBeanServer: MBeanServer = mock[MBeanServer]) = {

    implicit val config = new SiriusConfiguration
    config.setProp(SiriusConfiguration.MBEAN_SERVER, testMBeanServer)

    TestActorRef(new CompactionManager(siriusLog) {
      override def createCompactionActor = testCompactionActor
      override def startCompaction = {
        compactionsStarted += 1
        createCompactionActor
      }
      lastCompactionStarted = testLastStarted
      lastCompactionDuration = testLastDuration
      compactionActor = testCurrentCompactionActor
    })
  }

  def getCompactionInfo(mBeanServer: MBeanServer) = {
    val mBeanCaptor = ArgumentCaptor.forClass(classOf[Any])
    verify(mBeanServer).registerMBean(mBeanCaptor.capture(), any[ObjectName])
    mBeanCaptor.getValue.asInstanceOf[CompactionManagerInfoMBean]
  }

  before {
    compactionsStarted = 0
  }

  describe ("a CompactionManager") {
    describe ("when receiving CompactionComplete") {
      it ("must set compactionDuration and compactionActor") {
        val mBeanServer = mock[MBeanServer]
        val underTest = makeMockCompactionManager(testMBeanServer = mBeanServer,
          testCurrentCompactionActor = Some(TestProbe().ref),
          testLastStarted = Some(1L))

        val compactionInfo = getCompactionInfo(mBeanServer)
        val senderProbe = TestProbe()

        senderProbe.send(underTest, CompactionComplete)
        senderProbe.expectNoMsg(50.milliseconds)

        assert(None === compactionInfo.getCompactionActor)
        assert(System.currentTimeMillis() > compactionInfo.getLastCompactionDuration.get)
      }
    }

    describe ("when receiving Compact") {
      it ("must start compaction and record startTime if there is no current actor") {
        val mBeanServer = mock[MBeanServer]
        val underTest = makeMockCompactionManager(testMBeanServer = mBeanServer)
        val compactionInfo = getCompactionInfo(mBeanServer)
        val senderProbe = TestProbe()

        senderProbe.send(underTest, Compact)
        senderProbe.expectNoMsg(50.milliseconds)

        assert(None != compactionInfo.getCompactionActor)
        assert(None != compactionInfo.getLastCompactionStarted)
      }

      it ("must start compaction if the current actor is dead") {
        val compactionActor = TestActorRef(new CompactionActor(mock[SiriusLog]))
        val underTest = makeMockCompactionManager(testCurrentCompactionActor = Some(compactionActor))
        val senderProbe = TestProbe()

        compactionActor ! PoisonPill

        senderProbe.send(underTest, Compact)
        senderProbe.expectNoMsg(50.milliseconds)

        assert(1 === compactionsStarted)
      }

      it ("must do nothing if there is a live current actor") {
        val compactionActor = TestActorRef(new CompactionActor(mock[SiriusLog]))
        val underTest = makeMockCompactionManager(testCurrentCompactionActor = Some(compactionActor))
        val senderProbe = TestProbe()

        senderProbe.send(underTest, Compact)
        senderProbe.expectNoMsg(50.milliseconds)

        assert(0 === compactionsStarted)
      }
    }

  }

  override def afterAll() {
    actorSystem.shutdown()
  }
}
