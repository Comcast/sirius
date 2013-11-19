package com.comcast.xfinity.sirius.api.impl.membership

/**
 * Configuration object for cluster members.
 */
trait ClusterConfig {
  /**
   * List of akka paths for the members of the cluster.
   *
   * @return list of members
   */
  def members: List[String]
}
