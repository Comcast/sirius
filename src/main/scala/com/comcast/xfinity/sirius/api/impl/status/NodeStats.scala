package com.comcast.xfinity.sirius.api.impl.status

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
   * @param configMap config keys to values
   */
  case class NodeConfig(configMap: Map[String, Any]) {
    override def toString = {
      val sb = new StringBuilder
      sb.append("NodeConfig:\n")
      configMap.foreach(
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
          stats.foreach {
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