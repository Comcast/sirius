package com.comcast.xfinity.sirius.api.impl.membership

object BackwardsCompatibleClusterConfig {
  def apply(backend: ClusterConfig): BackwardsCompatibleClusterConfig = {
    new BackwardsCompatibleClusterConfig(backend)
  }
}

/**
 * Backwards-compatible ClusterConfig implementation. Will convert any "akka://" actor paths
 * into akka 2.2-compatible "akka.tcp://" paths. Wraps another concrete ClusterConfig implemntation.
 *
 * @param backend ClusterConfig supplying members
 */
private[membership] class BackwardsCompatibleClusterConfig(backend: ClusterConfig) extends ClusterConfig {
  /**
   * List of akka paths for the members of the cluster.
   *
   * @return list of members
   */
  def members = {
    backend.members.map {
      case path if path.startsWith("akka://") =>
        path.replaceFirst("akka://", "akka.tcp://")
      case path => path
    }
  }
}
