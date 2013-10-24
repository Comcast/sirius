package com.comcast.xfinity.sirius.uberstore

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.{TimedTest, NiceTest}
import akka.util.duration._
import akka.actor._
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import akka.testkit.{TestProbe, TestActorRef}
import javax.management.{ObjectName, MBeanServer}
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.uberstore.CompactionActor.CompactionComplete
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.comcast.xfinity.sirius.uberstore.CompactionManager.{ChildProvider, CompactionManagerInfoMBean, GetState, Compact}
import akka.util.Timeout
import scala.Some

class CompactionManagerTest extends NiceTest with BeforeAndAfterAll with TimedTest {

  implicit val timeout: Timeout = 5.seconds
  implicit var actorSystem: ActorSystem = ActorSystem("CompactionSchedulingTest")

  def makeMockCompactionManager(siriusLog: SiriusLog = mock[SiriusLog],
                                        testCompactionActor: ActorRef = TestProbe().ref,
                                        testCurrentCompactionActor: Option[ActorRef] = None,
                                        testLastStarted: Option[Long] = None,
                                        testLastDuration: Option[Long] = None,
                                        testMBeanServer: MBeanServer = mock[MBeanServer]) = {

    val config = new SiriusConfiguration
    config.setProp(SiriusConfiguration.MBEAN_SERVER, testMBeanServer)

    val childProvider = new ChildProvider(siriusLog: SiriusLog) {
      override def createCompactionActor()(implicit context: ActorContext) = testCompactionActor
    }
    TestActorRef(new CompactionManager(childProvider, config) {
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

        assert(waitForTrue(None == compactionInfo.getCompactionActor, 200, 10))
        assert(System.currentTimeMillis() > compactionInfo.getLastCompactionDuration.get)
      }
    }

    describe ("when receiving Terminated") {
      it ("must reset its compactionActor to None if the terminated actor matches compactionActor") {
        val startingCompactionActor = TestProbe()
        val underTest = makeMockCompactionManager(testCurrentCompactionActor = Some(startingCompactionActor.ref))

        underTest ! Terminated(startingCompactionActor.ref)
        assert(waitForTrue(None == underTest.underlyingActor.compactionActor, 1000, 25))
      }
      it ("must do nothing if the terminated actor does not match compactionActor") {
        val startingCompactionActor = TestProbe()
        val underTest = makeMockCompactionManager(testCurrentCompactionActor = Some(startingCompactionActor.ref))

        underTest ! Terminated(TestProbe().ref)

        Thread.sleep(50) // ugh. hard to test that an actor receives a msg and does nothing...
        assert(Some(startingCompactionActor.ref) === underTest.underlyingActor.compactionActor)
      }
    }

    describe ("when receiving Compact") {
      it ("must start compaction and record startTime if there is no current actor") {
        val underTest = makeMockCompactionManager()
        val senderProbe = TestProbe()

        senderProbe.send(underTest, Compact)

        assert(waitForTrue(None != underTest.underlyingActor.compactionActor, 200, 10))
        assert(waitForTrue(None != underTest.underlyingActor.lastCompactionStarted, 200, 10))
      }

      it ("must do nothing if there is a current CompactionActor") {
        val newCompactionActor = TestProbe()
        val underTest = makeMockCompactionManager(testCurrentCompactionActor = Some(TestProbe().ref),
                                                  testCompactionActor = newCompactionActor.ref)
        val senderProbe = TestProbe()

        senderProbe.send(underTest, Compact)
        senderProbe.expectNoMsg(50.milliseconds)

        newCompactionActor.expectNoMsg()
      }
    }

  }

  override def afterAll() {
    actorSystem.shutdown()
  }
}
