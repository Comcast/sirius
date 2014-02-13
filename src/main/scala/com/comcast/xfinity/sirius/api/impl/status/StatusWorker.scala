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
package com.comcast.xfinity.sirius.api.impl.status

import com.comcast.xfinity.sirius.api.SiriusConfiguration
import com.comcast.xfinity.sirius.admin.SiriusMonitorReader
import akka.actor.{Props, Actor}
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
   * Create Props for an actor of this type.
   * Create a StatusWorker, which will respond to and status queries
   *
   * @param supAddressString the string that this node identifies by,
   *          a smart engineer would pass in the supervisors external
   *          address ;)
   * @param config the node's configuration
   */
  def props(supAddressString: String, config: SiriusConfiguration): Props = {
    Props(classOf[StatusWorker], supAddressString, config, new SiriusMonitorReader)
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
                   monitorReader: SiriusMonitorReader) extends Actor {

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
