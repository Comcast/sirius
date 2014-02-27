/*
 *  Copyright 2012-2014 Comcast Cable Communications Management, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.comcast.xfinity.sirius.uberstore

import org.scalatest.BeforeAndAfterAll
import com.comcast.xfinity.sirius.TimedTest
import akka.actor._
import com.comcast.xfinity.sirius.NiceTest
import akka.actor.{ActorContext, ActorRef, ActorSystem}
import scala.concurrent.duration._
import com.comcast.xfinity.sirius.writeaheadlog.SiriusLog
import akka.testkit.{TestProbe, TestActorRef}
import javax.management.{ObjectName, MBeanServer}
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.uberstore.CompactionActor.CompactionComplete
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.comcast.xfinity.sirius.uberstore.CompactionManager.{ChildProvider, CompactionManagerInfoMBean, Compact}
import akka.util.Timeout
import scala.Some
import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver

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
    config.setProp(SiriusConfiguration.AKKA_EXTERNAL_ADDRESS_RESOLVER,AkkaExternalAddressResolver(actorSystem)(config))
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
        val compactionActor = TestProbe()
        val underTest = makeMockCompactionManager(testCompactionActor = compactionActor.ref)

        underTest ! Compact
        assert(waitForTrue(Some(compactionActor.ref) == underTest.underlyingActor.compactionActor, 1000, 25))

        actorSystem.stop(compactionActor.ref)

        assert(waitForTrue(None == underTest.underlyingActor.compactionActor, 1000, 25))
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
