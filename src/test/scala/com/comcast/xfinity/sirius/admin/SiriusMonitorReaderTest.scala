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
package com.comcast.xfinity.sirius.admin

import com.comcast.xfinity.sirius.NiceTest
import com.comcast.xfinity.sirius.admin.SiriusMonitorReaderTest.DummyMonitor
import com.comcast.xfinity.sirius.api.SiriusConfiguration
import javax.management.{MBeanException, ObjectName, MBeanServerFactory, MBeanServer}

object SiriusMonitorReaderTest {
  trait DummyMonitorMBean {
    def getCash: String
  }

  class DummyMonitor(value: Either[Throwable, String]) extends DummyMonitorMBean {
    def getCash = value match {
      case Right(toReturn) => toReturn
      case Left(toThrow) => throw toThrow
    }
  }

}

class SiriusMonitorReaderTest extends NiceTest {

  val underTest = new SiriusMonitorReader

  var mBeanServer: MBeanServer = _

  before {
    mBeanServer = MBeanServerFactory.createMBeanServer
  }

  after {
    MBeanServerFactory.releaseMBeanServer(mBeanServer)
  }

  it ("must do nothing if no MBeanServer is configured") {
    val underTest = new SiriusMonitorReader

    val config = new SiriusConfiguration

    assert(None === underTest.getMonitorStats(config))
  }

  it ("must expose metrics that exist") {
    val goodObjectName = new ObjectName("com.comcast.xfinity.sirius:type=GoodJawn")
    mBeanServer.registerMBean(new DummyMonitor(Right("Money")), goodObjectName)

    val config = new SiriusConfiguration
    config.setProp(SiriusConfiguration.MBEAN_SERVER, mBeanServer)

    val expected = Map[String, Map[String, Any]](
      "com.comcast.xfinity.sirius:type=GoodJawn" -> Map("Cash" -> "Money")
    )

    assert(Some(expected) === underTest.getMonitorStats(config))
  }

  it ("must sub in the exception as a string if a query fails") {
    val anException = new Exception("BLOOOD")
    val badObjectName = new ObjectName("com.comcast.xfinity.sirius:type=BadJawn")
    mBeanServer.registerMBean(new DummyMonitor(Left(anException)), badObjectName)

    val config = new SiriusConfiguration
    config.setProp(SiriusConfiguration.MBEAN_SERVER, mBeanServer)

    // XXX: it appears that this is how the MBeanServer is putting together the exception,
    //      just go with it
    val expectedException = new MBeanException(anException, anException.toString)
    val expected = Map[String, Map[String, Any]](
      ("com.comcast.xfinity.sirius:type=BadJawn" -> Map("Cash" -> expectedException.toString))
    )

    assert(Some(expected) === underTest.getMonitorStats(config))
  }

  it ("must not return anything for other MBeans") {
    val objectName = new ObjectName("com.comcast.xfinity.not.sirius:type=OtherJawn")
    mBeanServer.registerMBean(new DummyMonitor(Right("Money")), objectName)

    val config = new SiriusConfiguration
    config.setProp(SiriusConfiguration.MBEAN_SERVER, mBeanServer)

    val expected = Map[String, Map[String, Any]]()

    assert(Some(expected) === underTest.getMonitorStats(config))
  }
}
