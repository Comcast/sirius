package com.comcast.xfinity.sirius.api

import scala.reflect.BeanProperty

object SiriusConfiguration {

  /**
   * Factory method to create a SiriusConfiguration object.
   *
   * @param hostName the hostName or IP to which this instance should bind.  It is important that other
   *          Sirius instances identify this host by this name.  This is passed directly to Akka's
   *          configuration, for the interested
   * @param port the port which this instance should bind to.  This is passed directly to Akka's
   *          configuration, for the interested
   * @param clusterConfigPath string pointing to the location of this cluster's configuration.  This should
   *          be a file with Akka style addresses on each line indicating membership. For more information
   *          see http://doc.akka.io/docs/akka/snapshot/general/addressing.html
   * @param usePaxos should the underlying implementation use Paxos for ordering events? If true it will,
   *          if not it will use use a simple monotonically increasing counter, which is good enough
   *          as long as this instance isn't clustered
   * @param logLocation location on the filesystem of the Sirius persistent log.
   */
  def create(host: String,
             port: Int,
             clusterConfigPath: String,
             usePaxos: Boolean,
             logLocation: String): SiriusConfiguration = {
    val conf = new SiriusConfiguration()
    conf.host = host;
    conf.port = port;
    conf.clusterConfigPath = clusterConfigPath
    conf.usePaxos = usePaxos
    conf.logLocation = logLocation
    conf
  }

  /**
   * Scala syntax sugar friendly version of create
   * @see create
   */
  def apply(host: String,
            port: Int,
            clusterConfigPath: String,
            usePaxos: Boolean,
            logLocation: String): SiriusConfiguration =
    create(host, port, clusterConfigPath, usePaxos, logLocation)

}

/**
 * Configuration object for Sirius.  See individual attributes for more information.
 */
// XXX: scaladoc on these bean properties is sort of awkward...
class SiriusConfiguration {
  @BeanProperty var host: String = _
  @BeanProperty var port: Int = _
  @BeanProperty var clusterConfigPath: String = _
  @BeanProperty var usePaxos: Boolean = true
  @BeanProperty var logLocation: String = _
}