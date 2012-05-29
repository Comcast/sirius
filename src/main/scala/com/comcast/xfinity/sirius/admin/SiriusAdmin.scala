package com.comcast.xfinity.sirius.admin
import javax.management.MBeanServer
import java.lang.management.ManagementFactory
import javax.management.ObjectName
import java.net.InetAddress

/**
 * Hooks to register and unregister MBean(s) used for admin stuff with 
 * the passed in MBeanServer
 */
class SiriusAdmin(val port: Int, val mbeanServer : MBeanServer) {
    
  val hostName =  InetAddress.getLocalHost().getHostName();
  val mbeanName = new ObjectName("com.comcast.xfinity.sirius:type=SiriusInfo")

  def registerMbeans() = {
    val infoMbean = new SiriusInfo(port, hostName)
    mbeanServer.registerMBean(infoMbean, mbeanName)
  }
  
  def unregisterMbeans() = {
    mbeanServer.unregisterMBean(mbeanName)
  }
  
}