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

import collection.SortedMap

/**
 * General status case classes. The idea is that you can
 * call toString on these at it will look pretty.
 */
object NodeStats {

  /**
   * Encapsulates all possible stats, this is the catch all beast
   *
   * @param nodeName string address as this node is known externally
   * @param memUsage memory usage information
   * @param config node configuration
   * @param monitors monitor statistics
   */
  case class FullNodeStatus(nodeName: String,
                            memUsage: MemoryUsage,
                            config: NodeConfig,
                            monitors: MonitorStats)

  /**
   * Memory usage information
   *
   * @param freeMemory free memory on the system
   * @param totalMemory total available memory
   */
  case class MemoryUsage(freeMemory: Long, totalMemory: Long) {
    override def toString =
      "MemoryUsage: " + freeMemory + " free, " + totalMemory + " total"
  }

  /**
   * Configuration information
   *
   * @param configMap config keys to values, in string format
   */
  case class NodeConfig(configMap: Map[String, String]) {
    override def toString = {
      val sb = new StringBuilder
      sb.append("NodeConfig:\n")

      val sortedConfigMap = SortedMap(configMap.toArray:_*)
      sortedConfigMap.foreach(
        kv => sb.append("  %15s: %s\n".format(kv._1, kv._2))
      )
      sb.mkString
    }
  }

  /**
   * Monitor information
   *
   * @param statsOpt None if monitoring is not configured, Some(stats) if so,
   *          where stats is a collection of objectName -> (attribute -> value)
   */
  case class MonitorStats(statsOpt: Option[Map[String, Map[String, Any]]]) {
    override def toString = {
      val sb = new StringBuilder
      sb.append("Monitors:\n")
      statsOpt match {
        case None => "Not Configured"
        case Some(stats) =>
          val sortedStats = SortedMap(stats.toArray:_*)
          sortedStats.foreach {
            case (objName, attrs) => {
              sb.append("  %s:\n".format(objName))
              attrs.foreach(
                kv =>
                  sb.append("    %-15s: %s\n".format(kv._1, kv._2))
              )
            }
          }
      }
      sb.mkString
    }


  }
}
