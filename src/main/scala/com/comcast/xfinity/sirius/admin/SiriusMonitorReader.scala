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
import collection.JavaConversions.asScalaSet
import javax.management.{ObjectName, MBeanServer}

/**
 * Service class for querying an MBean server attached to a
 * SiriusConfiguration instance
 */
class SiriusMonitorReader {

  private final val siriusDomainQuery =
    new ObjectName("com.comcast.xfinity.sirius:*")

  /**
   * If config is configured with an MBeanServer, then read all
   * attributes for all MBeans under the "com.comcast.xfinity.sirius"
   * domain.
   *
   * @param config SiriusConfiguration
   * @return
   *          None if config is not configured with an MBeanServer
   *          Some(Map[String, Map[String, Any]]) if config is configured
   *            with an MBeanServer. The keys are object names, and the values
   *            are maps of attributes to values for the associated object
   *            name.
   *            If there is error accessing an object then
   *            Map("error" -> exceptionString) is returned.  If there is an
   *            error accessing an MBean attribute then the string cause of
   *            the exception is returned for that key.
   */
  def getMonitorStats(config: SiriusConfiguration): Option[Map[String, Map[String, Any]]] = {
    val mbeanServer: Option[MBeanServer] = config.getProp(SiriusConfiguration.MBEAN_SERVER)
    mbeanServer match {
      case Some(server) => Some(readMonitors(server))
      case None => None
    }
  }

  private def readMonitors(mBeanServer: MBeanServer) = {
    val objectNames = asScalaSet(mBeanServer.queryNames(siriusDomainQuery, null))

    objectNames.foldLeft(Map[String, Map[String, Any]]()) (
      (monitorsMap, objectName) =>
        monitorsMap +
          (objectName.toString -> getAttributesSafe(mBeanServer, objectName))
    )
  }

  // XXX: below we return the exception as a String instead of as an instance for simplicity-
  //      all we will be doing is looking at the string value anyway, if we return the exception
  //      we can throw off the client machine if the class for the exception isn't available!
  private def getAttributesSafe(mBeanServer: MBeanServer, objectName: ObjectName): Map[String, Any] =
    try {
      val mBeanInfo = mBeanServer.getMBeanInfo(objectName)
      mBeanInfo.getAttributes.foldLeft(Map[String, Any]()) (
        (attrMap, attrInfo) => {
          val attrName = attrInfo.getName
          attrMap + (attrName -> getAttributeSafe(mBeanServer, objectName, attrName))
        }
      )
    } catch {
      case anyException: Throwable => Map("error" -> anyException.toString)
    }

  // XXX: below we return the exception as a String instead of as an instance for simplicity-
  //      all we will be doing is looking at the string value anyway, if we return the exception
  //      we can throw off the client machine if the class for the exception isn't available!
  private def getAttributeSafe(mBeanServer: MBeanServer, objectName: ObjectName, attrName: String): Any =
    try {
      mBeanServer.getAttribute(objectName, attrName)
    } catch {
      case anyException: Throwable => anyException.toString
    }
}
