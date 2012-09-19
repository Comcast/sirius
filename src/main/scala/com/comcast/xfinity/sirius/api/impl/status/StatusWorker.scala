package com.comcast.xfinity.sirius.api.impl.status

import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.admin.SiriusMonitorReader
import akka.actor.Actor
import com.comcast.xfinity.sirius.api.impl.status.NodeStats._

object StatusWorker {

  // trait this in case we want to add different
  //  kinds of more specific inqueries
  /**
   * Trait encapsulating all status inqueries
   */
  sealed trait StatusQuery

  /**
   * General Status inquery, will return everything
   */
  case object GetStatus extends StatusQuery

  /**
   * Create a StatusWorker, which will respond to and status queries
   *
   * @param supAddressString the string that this node identifies by,
   *          a smart engineer would pass in the supervisors external
   *          address ;)
   * @param config the node's configuration
   */
  def apply(supAddressString: String, config: SiriusConfiguration) = {
    new StatusWorker(supAddressString, config)
  }
}

/**
 * Actor for receiving and replying to status inquery messages
 *
 * Prefer companion object construction over this
 *
 * @param config Sirius's Configuration
 * @param supAddressString fully qualified string address of
 *          this systems supervisor, as it should be reference externally
 * @param monitorReader SiriusMonitorReader for reading local
 *          registered MBeans, if such configured
 */
class StatusWorker(supAddressString: String,
                   config: SiriusConfiguration,
                   monitorReader: SiriusMonitorReader = new SiriusMonitorReader)
    extends Actor {

  import StatusWorker._

  def receive = {
    case GetStatus =>
      val memStats = getMemStats
      val configMap = getConfigMap
      val monitorStats = getMonitorStats

      sender ! FullNodeStatus(supAddressString, memStats, configMap, monitorStats)
  }

  private def getMemStats = {
    val runtime = Runtime.getRuntime
    MemoryUsage(runtime.freeMemory, runtime.totalMemory)
  }

  private def getConfigMap = {
    val configMap = config.getConfigMap
    NodeConfig(configMap.map(kv => (kv._1, kv._2.toString)))
  }

  private def getMonitorStats = MonitorStats(monitorReader.getMonitorStats(config))
}