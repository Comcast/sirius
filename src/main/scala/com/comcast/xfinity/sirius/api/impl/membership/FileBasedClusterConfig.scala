package com.comcast.xfinity.sirius.api.impl.membership

import scalax.file.Path
import scalax.io.Line.Terminators.NewLine

object FileBasedClusterConfig {
  def apply(config: String): FileBasedClusterConfig = {
    val configFile = Path.fromString(config)

    if (!configFile.exists) {
      throw new IllegalStateException("ClusterConfig file not found at location %s, cannot boot.".format(config))
    }

    new FileBasedClusterConfig(configFile)
  }
}

/**
 * ClusterConfig based on a static file. File will be re-read each time members is accessed.
 *
 * @param config Path of config file
 */
private[membership] class FileBasedClusterConfig(config: Path) extends ClusterConfig {

  /**
   * List of akka paths for the members of the cluster.
   *
   * @return list of members
   */
  def members = {
    config.lines(NewLine, includeTerminator = false)
      .toList
      .filterNot(_.startsWith("#"))
  }
}
