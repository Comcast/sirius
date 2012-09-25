package com.comcast.xfinity.sirius.admin

import com.comcast.xfinity.sirius.api.SiriusConfiguration
import javax.management.{ObjectName, MBeanServer}
import com.comcast.xfinity.sirius.util.AkkaExternalAddressResolver
import java.util.{Hashtable => JHashtable}
import akka.actor.{ActorContext, Address, ActorRef, ActorSystem}

/**
 * Trait for easily registering MBeans from an Actor
 *
 * Provides the methods registerMonitor to register an MBean, and unregisterMonitors
 * to unregister all MBeans registered by this instance
 */
trait MonitoringHooks {

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
   * @param siriusConfiguration Sirius's configuration, from which it is decided
   *          if mbean should be registered as an MBean
   * @param context the ActorContext this is run from, note it is necessary that this
   *          is run within an Actor for this to be available
   */
  def registerMonitor(mbean: => Any, siriusConfig: SiriusConfiguration)(implicit context: ActorContext) {
    siriusConfig.getProp[MBeanServer](SiriusConfiguration.MBEAN_SERVER) match {
      case Some(mbeanServer) =>
        val objectName = getObjectName(mbean, context.self, context.system)
        mbeanServer.registerMBean(mbean, objectName)
        objectNames += objectName
      case None => // no-op
    }
  }

  /**
   * Unregister all MBeans registered by this instance
   *
   * @param siriusConfiguration Sirius's configuration, from which it is decided
   *          if mbean should be registered as an MBean
   */
  def unregisterMonitors(siriusConfig: SiriusConfiguration) {
    siriusConfig.getProp[MBeanServer]("sirius.monitoring.mbean-server") match {
      case Some(mbeanServer) =>
        objectNames.foreach(mbeanServer.unregisterMBean(_))
      case None => // no-op
    }
  }

  private def getObjectName(mbean: Any, actor: ActorRef, actorSystem: ActorSystem): ObjectName = {
    val kvs = new JHashtable[String, String]
    kvs.put("path", "/" + actor.path.elements.reduceLeft(_ + "/" + _))

    val (host, port) = getHostPort(actorSystem)
    kvs.put("host", host)
    kvs.put("port", port)
    kvs.put("sysname", actorSystem.name)

    val statClass = mbean.getClass.getSimpleName
    kvs.put("name", statClass)

    new ObjectName("com.comcast.xfinity.sirius", kvs)
  }

  private def getHostPort(actorSystem: ActorSystem): (String, String) =
    AkkaExternalAddressResolver(actorSystem).externalAddress match {
        case None => ("", "")
        case Some(address) =>
          val host = address.host.getOrElse("")
          val port = address.port match {
            case None => ""
            case Some(portNo) => portNo.toString
          }
          (host, port)
      }
}