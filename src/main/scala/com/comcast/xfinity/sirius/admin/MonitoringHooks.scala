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

import com.comcast.xfinity.sirius.api.SiriusConfiguration
import javax.management.{ObjectName, MBeanServer}
import akka.actor.ActorContext

/**
 * Trait for easily registering MBeans from an Actor
 *
 * Provides the methods registerMonitor to register an MBean, and unregisterMonitors
 * to unregister all MBeans registered by this instance
 */
trait MonitoringHooks {

  private[admin] val objectNameHelper = new ObjectNameHelper

  private[admin] var objectNames = Set[ObjectName]()

  /**
   * If sirius is configured to do such, register mbean as an MBean
   *
   * The object name derived for this object is kept locally on this instance, and
   * will be unregistered when unregisterMonitors is called.
   *
   * @param mbean A call-by-name parameter to get the mbean to register, if
   *          sirisConfiguration is not configured with an MBeanFactory, then
   *          this is not used, and thus not called
   * @param siriusConfig Sirius's configuration, from which it is decided
   *          if mbean should be registered as an MBean
   * @param context the ActorContext this is run from, note it is necessary that this
   *          is run within an Actor for this to be available
   */
  def registerMonitor(mbean: => Any, siriusConfig: SiriusConfiguration)(implicit context: ActorContext) {
    siriusConfig.getProp[MBeanServer](SiriusConfiguration.MBEAN_SERVER) match {
      case Some(mbeanServer) =>
        val objectName = objectNameHelper.getObjectName(mbean, context.self, context.system)
        mbeanServer.registerMBean(mbean, objectName)
        objectNames += objectName
      case None => // no-op
    }
  }

  /**
   * Unregister all MBeans registered by this instance
   *
   * @param siriusConfig Sirius's configuration, from which it is decided
   *          if mbean should be registered as an MBean
   */
  def unregisterMonitors(siriusConfig: SiriusConfiguration) {
    siriusConfig.getProp[MBeanServer]("sirius.monitoring.mbean-server") match {
      case Some(mbeanServer) =>
        objectNames.foreach(mbeanServer.unregisterMBean(_))
      case None => // no-op
    }
  }

}
