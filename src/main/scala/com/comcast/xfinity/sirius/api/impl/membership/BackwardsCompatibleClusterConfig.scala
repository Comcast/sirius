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

import com.comcast.xfinity.sirius.api.SiriusConfiguration

object BackwardsCompatibleClusterConfig {
  def apply(backend: ClusterConfig)(implicit config: SiriusConfiguration): BackwardsCompatibleClusterConfig = {
    new BackwardsCompatibleClusterConfig(backend)(config)
  }
}

/**
 * Backwards-compatible ClusterConfig implementation. Will convert any "akka://" actor paths
 * into akka 2.2-compatible "akka.tcp://" paths. Wraps another concrete ClusterConfig implemntation.
 *
 * @param backend ClusterConfig supplying members
 */
private[membership] class BackwardsCompatibleClusterConfig(backend: ClusterConfig)(config: SiriusConfiguration) extends ClusterConfig {
  val prefix = config.getProp(SiriusConfiguration.ENABLE_SSL, false) match {
    case true => "akka.ssl.tcp://"
    case false => "akka.tcp://"
  }

  /**
   * List of akka paths for the members of the cluster.
   *
   * @return list of members
   */
  def members = {
    backend.members.map {
      case path if path.startsWith("akka://") =>
        path.replaceFirst("akka://", prefix)
      case path => path
    }
  }
}
