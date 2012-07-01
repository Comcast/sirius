package com.comcast.xfinity.sirius.admin

import com.comcast.xfinity.sirius.info.SiriusInfo

import javax.management.MBeanServer
import javax.management.ObjectName

/**
 * Hooks to register and unregister MBean(s) used for admin stuff with
 * the passed in MBeanServer
 */
class SiriusAdmin(val info: SiriusInfo, val mbeanServer: MBeanServer) {

  val mbeanName = new ObjectName("com.comcast.xfinity.sirius:type=SiriusInfo,name=" + info.hostName + "|" + info.port)

  def registerMbeans() { mbeanServer.registerMBean(info, mbeanName) }

  def unregisterMbeans() { mbeanServer.unregisterMBean(mbeanName) }
}