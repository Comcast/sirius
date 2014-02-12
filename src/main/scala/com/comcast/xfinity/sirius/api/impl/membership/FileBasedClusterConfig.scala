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
